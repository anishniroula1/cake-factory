import re

from doc2vec_completed.opensearch import constants
from ta_db_connection.atatables import TextSimilarityInput


class RRFFilterer:
    # Section 4 starts the refugee claim information that we need. When this
    # heading exists, sections 1 through 3 are removed before sentence splitting.
    pattern = (
        r"4[,.**\/**\s]{0,}"
        "(REFUGEE CLAIM|BASIS OF REFUGEE STATUS RECOGNITION|"
        "Summary of the Basis of The Primary Individual)"
    )

    def _filter_sections_one_through_three(self, string):
        if re.search(self.pattern, string):
            start = re.search(self.pattern, string).start()
            string = string[start::]
            return string
        else:
            return string

    def _filter_section_five(self, string):
        # Section 5 is not used for sentence matching. Join the content before
        # section 5 with section 6 and everything that follows it.
        section5pattern = (
            r"5[,.**\/**\s]{0,}"
            "(NEED FOR RESETTLEMENT|RESETTLEMENT NEEDS AND SUBMISSION PRIORITY)"
        )
        section6pattern = r"6[,.**\/**\s]{0,}(SPECIFIC NEEDS|SPECI FIC NEEDS)"
        if re.search(section5pattern, string) and re.search(section6pattern, string):
            section5start = re.search(section5pattern, string).start()
            section6start = re.search(section6pattern, string).start()
            string = string[0:section5start] + string[section6start::]
            return string
        else:
            return string

    def _filter_sections(self, string):
        string = self._filter_sections_one_through_three(string)
        string = self._filter_section_five(string)
        return string

    def filter_rrfs(
        self,
        raw_document: TextSimilarityInput,
    ) -> TextSimilarityInput:
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
