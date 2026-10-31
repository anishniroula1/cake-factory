import re

from doc2vec_completed.opensearch import constants
from ta_db_connection.atatables import TextSimilarityInput
from ta_utils.logger.CustomLogger import CustomLogger


class RRFFilterer:
    """Remove RRF template sections that should not become searchable sentences."""

    # Section 4 starts the refugee claim information that we need. When this
    # heading exists, sections 1 through 3 are removed before sentence splitting.
    pattern = (
        r"4[,.**\/**\s]{0,}"
        "(REFUGEE CLAIM|BASIS OF REFUGEE STATUS RECOGNITION|"
        "Summary of the Basis of The Primary Individual)"
    )

    def __init__(self, custom_logger: CustomLogger) -> None:
        self.logger = custom_logger

    def _filter_sections_one_through_three(self, content: str) -> str:
        """Keep the RRF content beginning with section 4.

        Input: sections 1 through 4 of a Refugee RRF document.
        Output: section 4 and all content that follows it. The original content
        is returned when a supported section 4 heading is not found.
        """

        section_four_match = re.search(self.pattern, content)
        if section_four_match:
            self.logger.info(
                "RRF section 4 was detected. Sections 1 through 3 were removed."
            )
            start = section_four_match.start()
            content = content[start::]
            return content
        else:
            return content

    def _filter_section_five(self, content: str) -> str:
        """Remove section 5 while preserving section 6 and later content.

        Input: an RRF document containing sections 5 and 6.
        Output: the content before section 5 joined to section 6. The original
        content is returned unless both section headings are found.
        """

        # Section 5 is not used for sentence matching. Join the content before
        # section 5 with section 6 and everything that follows it.
        section5pattern = (
            r"5[,.**\/**\s]{0,}"
            "(NEED FOR RESETTLEMENT|RESETTLEMENT NEEDS AND SUBMISSION PRIORITY)"
        )
        section6pattern = r"6[,.**\/**\s]{0,}(SPECIFIC NEEDS|SPECI FIC NEEDS)"
        section_five_match = re.search(section5pattern, content)
        section_six_match = re.search(section6pattern, content)
        if section_five_match and section_six_match:
            self.logger.info(
                "RRF sections 5 and 6 were detected. Section 5 was removed."
            )
            section5start = section_five_match.start()
            section6start = section_six_match.start()
            content = content[0:section5start] + content[section6start::]
            return content
        else:
            return content

    def _filter_sections(self, content: str) -> str:
        """Apply both RRF cleanup rules in their document order.

        Input: the complete RRF text.
        Output: content starting at section 4 with section 5 removed when the
        required headings are present.
        """

        content = self._filter_sections_one_through_three(content)
        content = self._filter_section_five(content)
        return content

    def filter_rrfs(
        self,
        raw_document: TextSimilarityInput,
    ) -> TextSimilarityInput:
        """Filter configured Refugee RRF documents and leave all others unchanged.

        Input: a TextSimilarityInput record containing document content,
        section_name, and analysis_group.
        Output: the same record with filtered content only when section_name is
        an RRF section and analysis_group is Refugee.
        """

        # Apply this cleanup only to configured Refugee RRF sections. Every
        # other document keeps its original content without any modification.
        original_content = raw_document.content

        raw_document.content = (
            self._filter_sections(raw_document.content)
            if (
                raw_document.section_name.lower()
                in list(
                    map(
                        str.lower,
                        constants.CASE_REFUGEE_SECTION_NAMES,
                    )
                )
            )
            & (raw_document.analysis_group == "Refugee")
            else original_content
        )

        return raw_document
