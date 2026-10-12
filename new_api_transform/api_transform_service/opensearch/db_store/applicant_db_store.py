from collections.abc import Callable

from sqlalchemy import bindparam, text
from sqlalchemy.orm import Session

LOAD_APPLICANT_DETAILS = text(
    """
    WITH residence AS (
        SELECT DISTINCT ON (application_id) *
        FROM biodata.address
        WHERE is_current = TRUE AND address_type_cd = 1
        ORDER BY application_id, last_modified DESC
    ),
    mailing AS (
        SELECT DISTINCT ON (application_id) *
        FROM biodata.address
        WHERE is_current = TRUE AND address_type_cd = 2
        ORDER BY application_id, last_modified DESC
    )
    SELECT
        applicant.application_id,
        applicant.first_name,
        applicant.middle_name,
        applicant.last_name,
        applicant.second_surname,
        CASE WHEN LENGTH(applicant.dob) = 10 THEN applicant.dob ELSE '' END AS dob,
        applicant.country_of_birth,
        applicant.country_of_citizenship,
        applicant.rsc_region,
        applicant.uscis_interviewer,
        applicant.current_country,
        applicant.priority,
        applicant.case_groups,
        applicant.case_members,
        applicant.case_status,
        applicant.program,
        applicant.email_address,
        applicant.phone_number,
        applicant.last_modified,
        mailing.address AS mailing_address_line1,
        mailing.apt AS mailing_address_line2,
        mailing.city AS mailing_address_city,
        mailing.state AS mailing_address_state,
        mailing.zip AS mailing_address_zip,
        residence.address AS residence_address_line1,
        residence.apt AS residence_address_line2,
        residence.city AS residence_address_city,
        residence.state AS residence_address_state,
        residence.zip AS residence_address_zip,
        applicant.source_type,
        applicant.first_name_ocr,
        applicant.middle_name_ocr,
        applicant.last_name_ocr,
        applicant.dob_ocr,
        applicant.country_of_citizenship_ocr,
        residence.address_ocr AS residence_address_ocr,
        residence.apt_ocr AS residence_apt_ocr,
        residence.city_ocr AS residence_city_ocr,
        residence.state_ocr AS residence_state_ocr,
        residence.zip_ocr AS residence_zip_ocr,
        mailing.address_ocr AS mailing_address_ocr,
        mailing.apt_ocr AS mailing_apt_ocr,
        mailing.city_ocr AS mailing_city_ocr,
        mailing.state_ocr AS mailing_state_ocr,
        mailing.zip_ocr AS mailing_zip_ocr
    FROM biodata.applicant AS applicant
    LEFT JOIN residence ON residence.application_id = applicant.application_id
    LEFT JOIN mailing ON mailing.application_id = applicant.application_id
    LEFT JOIN api.transform_status AS status ON status.application_id = applicant.application_id
    WHERE applicant.application_id IN :application_ids
      AND applicant.is_current = TRUE
      AND (
          :reduce_kafka_size = FALSE
          OR GREATEST(
              applicant.last_modified,
              residence.last_modified,
              mailing.last_modified
          ) > COALESCE(status.last_transmission, TIMESTAMPTZ '1970-01-01 00:00:00+00')
      )
    """
).bindparams(bindparam("application_ids", expanding=True))

NO_OCR_CALCULATION_SOURCE_TYPES = {"GLOBAL", "GLOBAL_RAD"}


class ApplicantDBStore:
    def __init__(self, session_factory: Callable[[], Session]) -> None:
        """Keep applicant, address, and OCR work in one readable place."""

        self.session_factory = session_factory

    def load_applicant_details(self, application_ids: list[int], reduce_kafka_size: bool) -> dict[int, dict]:
        """Load applicant details and add the five existing OCR fault flags.

        Input: application IDs [10, 20].
        Output: a dictionary keyed by application ID for quick payload assembly.
        """

        if not application_ids:
            return {}
        with self.session_factory() as session:
            rows = (
                session.execute(
                    LOAD_APPLICANT_DETAILS, {"application_ids": application_ids, "reduce_kafka_size": reduce_kafka_size}
                )
                .mappings()
                .all()
            )
        applicant_details = {}
        for row in rows:
            values = dict(row)
            application_id = int(values.pop("application_id"))
            applicant_details[application_id] = self._build_applicant(values)
        return applicant_details

    def _build_applicant(self, row: dict) -> dict:
        """Remove OCR source columns after turning them into UI fault flags."""

        source_type = row.pop("source_type")
        if source_type in NO_OCR_CALCULATION_SOURCE_TYPES:
            faults = self._empty_ocr_faults()
        else:
            faults = {
                "ocr_name_fault": self.calculate_ocr(
                    [row.pop("first_name_ocr"), row.pop("middle_name_ocr"), row.pop("last_name_ocr")]
                ),
                "ocr_residence_address_fault": self.calculate_ocr(
                    [row.pop(name) for name in self._residence_ocr_fields()]
                ),
                "ocr_mailing_address_fault": self.calculate_ocr([row.pop(name) for name in self._mailing_ocr_fields()]),
                "ocr_dob_fault": self.calculate_ocr(row.pop("dob_ocr")),
                "ocr_country_of_citizenship_fault": self.calculate_ocr(row.pop("country_of_citizenship_ocr")),
            }
        # GLOBAL sources skip calculation, but their private OCR columns still
        # need to be removed from the API payload.
        for name in self._all_ocr_fields():
            row.pop(name, None)
        if row.get("last_modified") is not None:
            row["last_modified"] = row["last_modified"].strftime("%Y-%m-%d %H:%M:%S.%f") + "+00"
        row.update(faults)
        return row

    @staticmethod
    def calculate_ocr(values: object) -> bool:
        """Return true when more than ten percent of available OCR bits are 1.

        Input: "000100". Output: true because one of six bits is above 10%.
        """

        if values is None:
            return False
        if isinstance(values, str):
            zeros = values.count("0")
            ones = values.count("1")
            return zeros + ones > 0 and ones / (zeros + ones) > 0.1
        return any(ApplicantDBStore.calculate_ocr(value) for value in values)

    @staticmethod
    def _empty_ocr_faults() -> dict[str, bool]:
        return {
            "ocr_name_fault": False,
            "ocr_residence_address_fault": False,
            "ocr_mailing_address_fault": False,
            "ocr_dob_fault": False,
            "ocr_country_of_citizenship_fault": False,
        }

    @staticmethod
    def _residence_ocr_fields() -> list[str]:
        return [
            "residence_address_ocr",
            "residence_apt_ocr",
            "residence_city_ocr",
            "residence_state_ocr",
            "residence_zip_ocr",
        ]

    @staticmethod
    def _mailing_ocr_fields() -> list[str]:
        return ["mailing_address_ocr", "mailing_apt_ocr", "mailing_city_ocr", "mailing_state_ocr", "mailing_zip_ocr"]

    @classmethod
    def _all_ocr_fields(cls) -> list[str]:
        return [
            "first_name_ocr",
            "middle_name_ocr",
            "last_name_ocr",
            "dob_ocr",
            "country_of_citizenship_ocr",
            *cls._residence_ocr_fields(),
            *cls._mailing_ocr_fields(),
        ]
