from collections.abc import Callable

from sqlalchemy import bindparam, text
from sqlalchemy.orm import Session

# Load the current applicant and the newest current residence and mailing
# addresses for each requested application.
#
# Input:
#   {"application_ids": [50], "reduce_kafka_size": false}
#
# Tables used:
#   biodata.applicant supplies applicant details and OCR source values.
#   biodata.address supplies address details and address OCR source values.
#   api.transform_status supplies the last successful Kafka transmission time.
#
# Output example:
#   {
#       "application_id": 50,
#       "first_name": "Ana",
#       "mailing_address_city": "Arlington",
#       "residence_address_city": "Alexandria",
#       "first_name_ocr": "000100"
#   }
#
# When reduce_kafka_size is true, the query returns the applicant only when the
# applicant or either address changed after the last successful transmission.
# The Python store converts the OCR source values into five API fault flags.
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
    """Load applicant data and convert OCR source values into API fault flags."""

    def __init__(self, session_factory: Callable[[], Session]) -> None:
        self.session_factory = session_factory

    def load_applicant_details(self, application_ids: list[int], reduce_kafka_size: bool) -> dict[int, dict]:
        """Load current applicant and address details with the five OCR fault flags.

        Input: application_ids=[10, 20], reduce_kafka_size=False
        Output: {10: {"first_name": "Ana", "ocr_name_fault": False, ...},
        20: {...}}

        When reduce_kafka_size is True, unchanged applicant records can be
        omitted by the database query.
        """

        if not application_ids:
            return {}
        with self.session_factory() as session:
            applicant_records = (
                session.execute(
                    LOAD_APPLICANT_DETAILS, {"application_ids": application_ids, "reduce_kafka_size": reduce_kafka_size}
                )
                .mappings()
                .all()
            )
        applicant_payloads = {}
        for applicant_record in applicant_records:
            applicant_payload = dict(applicant_record)
            application_id = int(applicant_payload.pop("application_id"))
            applicant_payloads[application_id] = self._build_applicant_payload(applicant_payload)
        return applicant_payloads

    def _build_applicant_payload(self, applicant_payload: dict) -> dict:
        """Convert OCR source columns into UI fault flags and remove private fields.

        Input: applicant_payload containing first_name_ocr="000100" and other applicant values.
        Output: applicant values containing ocr_name_fault=True without OCR bit fields.
        """

        source_type = applicant_payload.pop("source_type")
        if source_type in NO_OCR_CALCULATION_SOURCE_TYPES:
            ocr_faults = self._empty_ocr_faults()
        else:
            ocr_faults = {
                "ocr_name_fault": self.has_ocr_fault(
                    [
                        applicant_payload.pop("first_name_ocr"),
                        applicant_payload.pop("middle_name_ocr"),
                        applicant_payload.pop("last_name_ocr"),
                    ]
                ),
                "ocr_residence_address_fault": self.has_ocr_fault(
                    [applicant_payload.pop(name) for name in self._residence_ocr_fields()]
                ),
                "ocr_mailing_address_fault": self.has_ocr_fault(
                    [applicant_payload.pop(name) for name in self._mailing_ocr_fields()]
                ),
                "ocr_dob_fault": self.has_ocr_fault(applicant_payload.pop("dob_ocr")),
                "ocr_country_of_citizenship_fault": self.has_ocr_fault(
                    applicant_payload.pop("country_of_citizenship_ocr")
                ),
            }
        # GLOBAL sources skip calculation, but their private OCR columns still
        # need to be removed from the API payload.
        for name in self._all_ocr_fields():
            applicant_payload.pop(name, None)
        if applicant_payload.get("last_modified") is not None:
            applicant_payload["last_modified"] = (
                applicant_payload["last_modified"].strftime("%Y-%m-%d %H:%M:%S.%f") + "+00"
            )
        applicant_payload.update(ocr_faults)
        return applicant_payload

    @staticmethod
    def has_ocr_fault(ocr_values: object) -> bool:
        """Return whether more than ten percent of available OCR bits are 1.

        Input: values="000100"
        Output: True because one of six available bits is 1.
        """

        if ocr_values is None:
            return False
        if isinstance(ocr_values, str):
            zeros = ocr_values.count("0")
            ones = ocr_values.count("1")
            return zeros + ones > 0 and ones / (zeros + ones) > 0.1
        return any(ApplicantDBStore.has_ocr_fault(ocr_value) for ocr_value in ocr_values)

    @staticmethod
    def _empty_ocr_faults() -> dict[str, bool]:
        """Return all five OCR fault fields set to False."""

        return {
            "ocr_name_fault": False,
            "ocr_residence_address_fault": False,
            "ocr_mailing_address_fault": False,
            "ocr_dob_fault": False,
            "ocr_country_of_citizenship_fault": False,
        }

    @staticmethod
    def _residence_ocr_fields() -> list[str]:
        """Return the OCR source columns used for the residence address flag."""

        return [
            "residence_address_ocr",
            "residence_apt_ocr",
            "residence_city_ocr",
            "residence_state_ocr",
            "residence_zip_ocr",
        ]

    @staticmethod
    def _mailing_ocr_fields() -> list[str]:
        """Return the OCR source columns used for the mailing address flag."""

        return ["mailing_address_ocr", "mailing_apt_ocr", "mailing_city_ocr", "mailing_state_ocr", "mailing_zip_ocr"]

    @classmethod
    def _all_ocr_fields(cls) -> list[str]:
        """Return every private OCR source column removed from the API payload."""

        return [
            "first_name_ocr",
            "middle_name_ocr",
            "last_name_ocr",
            "dob_ocr",
            "country_of_citizenship_ocr",
            *cls._residence_ocr_fields(),
            *cls._mailing_ocr_fields(),
        ]
