#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""The module defines methods used to check the rules to be followed while indexing the objects to
    Enterprise Search.
"""
import re

from wcmatch import glob


class IndexingRules:
    """This class holds methods used to apply indexing filters on the documents to be indexed
    """

    def __init__(self, config):
        self.include = config.get_value("include")
        self.exclude = config.get_value("exclude")

    def should_ocr(self, file_details):
        should_include = False
        # should_exclude = False
        if self.include:
            include_ocr = {'ocr_path_template': self.include['ocr_path_template']}
            # should_index_by_default: True = do ocr if no pattern, False = DO NOT DO ocr if no pattern
            should_include, ocr_language = self.should_include_or_exclude_ocr(include_ocr, {}, file_details, 'include')
        # if self.exclude:
        #     # should_index_by_default: True = do ocr if no pattern, False = DO NOT DO ocr if no pattern
        #     exclude_ocr = {'ocr_path_template': self.exclude['ocr_path_template']}
        #     should_exclude = self.should_include_or_exclude_ocr(exclude_ocr, include_ocr, file_details, 'exclude')
        
        print('should_ocr', should_include, file_details['file_path'])
        return should_include, ocr_language
    
    def should_include_or_exclude_ocr(self, pattern_dict, is_present_in_include, file_details, pattern_type, should_ocr_by_default=False):
        should_ocr = should_ocr_by_default
        for filtertype, pattern in pattern_dict.items():
            for value in (pattern or []):
                if is_present_in_include and (value['path'] in (is_present_in_include.get(filtertype) or [])):
                    pattern.remove(value)
            should_ocr, ocr_language = self.follows_ocr_rule(pattern, file_details, pattern_type)
            if should_ocr is False:
                should_ocr = False
            elif should_ocr is True:
                return should_ocr, ocr_language
        return should_ocr, None
    
    def follows_ocr_rule(self, pattern, file_details, pattern_type):
        """Applies filters on the file and returns True or False based on whether
           it follows the pattern or not
            :param pattern: include/ exclude pattern provided for matching
            :param file_details: dictionary containing file properties
            :param pattern_type: include/exclude
        """
        if pattern:
            for value in pattern:
                path = value['path']
                lang = value.get('language') # optional

                # Define the pattern using regular expression
                pattern = r'sectionID=(\d+)'

                # Extract sectionID from the URL using regular expression
                match = re.search(path, pattern)
                if not match:
                    return False, None
                
                section_id = match.group(1)
                path_pattern = f'*sectionID={section_id}*'

                match = re.search(path, pattern)
                if match:
                    attachment_id = match.group(1)
                    path_pattern = path_pattern + f"attachmentID={attachment_id}*"
                
                result = glob.globmatch(file_details['file_path'], path_pattern, flags=glob.GLOBSTAR)
                if (pattern_type == 'include' and result) or (pattern_type == 'exclude' and not(result)):
                    return True, lang

        return False, None
