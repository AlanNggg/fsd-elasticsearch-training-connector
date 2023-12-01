import os
import sys
import tempfile

# sys.path.append("C:/LEADTOOLS22/Examples/Common/Python") 
from DemosTools import *
from leadtools import LibraryLoader
from UnlockSupport import Support

LibraryLoader.add_reference("Leadtools") 
from Leadtools import *
from Leadtools import RasterSupport

LibraryLoader.add_reference("Leadtools.Ocr") 
from Leadtools.Ocr import *

LibraryLoader.add_reference("Leadtools.Document") 
from Leadtools.Document import *

LibraryLoader.add_reference("Newtonsoft.Json") 
from Newtonsoft.Json import *
from System.Collections.Generic import *
from System.IO import *
from System.Net import *


class LeadTools:
    def __init__(self, config, logger):
        self.logger = logger
        self.config = config
        self.license = config.get_value('leadtools.license_path')
        self.startup_parameters = config.get_value('leadtoools.startup_parameters')

        self.enable_leadtools_ocr = config.get_value("enable_leadtools_ocr")

        if self.enable_leadtools_ocr:
            try:
                Support.set_license(self.license)
            except Exception as exception:
                self.logger.exception(f"Error while setting license to Leadtools. Error: {exception}")

    def connect(self):
        try:   
            ocr_engine = OcrEngineManager.CreateEngine(OcrEngineType.LEAD) 
            ocr_engine.Startup(None, None, None, self.startup_parameters) 

            ocr_engine.SettingManager.SetEnumValue("Recognition.RecognitionModuleTradeoff", 0)
            ocr_engine.SettingManager.SetBooleanValue("Recognition.Words.DiscardLowConfidenceZones", True)
            ocr_engine.SettingManager.SetBooleanValue("Recognition.Words.DiscardLowConfidenceWords", True)

            # ocr_engine.SettingManager.SetIntegerValue("Recognition.Words.LowWordConfidence", 60)
            ocr_engine.SettingManager.SetBooleanValue("Recognition.CharacterFilter.DiscardNoiseLikeCharacters", True)
            ocr_engine.SettingManager.SetBooleanValue("Recognition.CharacterFilter.DiscardNoisyZones", True)

            return ocr_engine
        except Exception as exception:
            self.logger.exception(f"Unknown error while connecting to Leadtools. Error: {exception}")
            raise exception   
          
    def run_leadtools_ocr_on_temp_file(self, ocr_engine, path, file_content, lang=None):
        text = ''
        file_extension = os.path.splitext(path)[1].lower()
        with tempfile.NamedTemporaryFile(delete=False, suffix=file_extension) as file_obj:
            file_name = file_obj.name
            file_obj.write(file_content)

        self.logger.info('run_leadtools_ocr_on_temp_file %s %s' % (file_name, file_extension))
        text = self.run_leadtools_ocr(ocr_engine, file_name, lang)
        os.remove(file_name)
        return text
    
    def run_leadtools_icr_on_temp_file(self, ocr_engine, path, file_content, lang=None):
        text = ''
        file_extension = os.path.splitext(path)[1].lower()
        with tempfile.NamedTemporaryFile(delete=False, suffix=file_extension) as file_obj:
            file_name = file_obj.name
            file_obj.write(file_content)

        self.logger.info('run_leadtools_icr_on_temp_file %s %s' % (file_name, file_extension))
        text = self.run_leadtools_icr(ocr_engine, file_name, lang)
        os.remove(file_name)
        return text
    

    def run_leadtools_ocr(self, ocr_engine, file, lang=None):
        ocr_document = ocr_engine.DocumentManager.CreateDocument() 

        ocr_document.Pages.AddPages(file, 1, -1, None) 
        ocr_document.Pages.AutoZone(None) 

        if lang:
            ocr_engine.LanguageManager.EnableLanguages(lang.split(','))
        else:
            ocr_engine.LanguageManager.EnableLanguages(["en", "zh-Hant"])
        # supportedLanguages = ocr_engine.LanguageManager.GetSupportedLanguages()
        # for lang in supportedLanguages:
        #     print(lang)

        enabledLanguages = ocr_engine.LanguageManager.GetEnabledLanguages()
        for lang in enabledLanguages:
            # print(lang)
            self.logger.info(lang)

        ocr_document.Pages.Recognize(None)

        all_pages_text = ""
        for page in ocr_document.Pages: 
            # parse the text and build the DocumentPageText object 
            page_text = page.GetText(-1)
            all_pages_text += page_text

        ocr_document.Dispose()

        return all_pages_text

    def run_leadtools_icr(self, ocr_engine, file, lang=None):
        ocr_document = ocr_engine.DocumentManager.CreateDocument() 

        ocr_document.Pages.AddPages(file, 1, -1, None) 
        ocr_document.Pages.AutoZone(None) 

        if lang:
            ocr_engine.LanguageManager.EnableLanguages(lang.split(','))
        else:
            ocr_engine.LanguageManager.EnableLanguages(["en", "zh-Hant"])
        # supportedLanguages = ocr_engine.LanguageManager.GetSupportedLanguages()
        # for lang in supportedLanguages:
        #     print(lang)

        # enabledLanguages = ocr_engine.LanguageManager.GetEnabledLanguages()
        # for lang in enabledLanguages:
        #     # print(lang)
        #     self.logger.info(lang)


        for ocr_page in ocr_document.Pages:
            for i in range(ocr_page.Zones.Count):
                zone = ocr_page.Zones[i]
                zone.ZoneType = OcrZoneType.Icr
            
        ocr_document.Pages.Recognize(None)

        all_pages_text = ""
        for page in ocr_document.Pages: 
            # parse the text and build the DocumentPageText object 
            page_text = page.GetText(-1)
            all_pages_text += page_text

        ocr_document.Dispose()

        return all_pages_text
