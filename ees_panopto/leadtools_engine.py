import sys

# Import LEADTOOLS Demo common modules =
sys.path.append("C:/LEADTOOLS22/Examples/Common/Python") 
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


class LeadTools:
    def __init__(self, config, logger):
        self.logger = logger
        # Support.set_license("/fsd/LEADTOOLS22/Support/Common/License") 
        Support.set_license("C:/LEADTOOLS22/Support/Common/License") 

    def ocr_engine(self):
        if not self._ocr_engine:
            raise Exception('Cannot access ocr_engine before creating')
        return self._ocr_engine
    
    def connect(self):
        try:        
            ocr_engine = OcrEngineManager.CreateEngine(OcrEngineType.LEAD) 
            ocr_engine.Startup(None, None, None, r"C:\LEADTOOLS22\Bin\Common\OcrLEADRuntime") 

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
        
    # def close(self):
    #     ocr_engine = self.ocr_engine()
    #     ocr_engine.Dispose()

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

        enabledLanguages = ocr_engine.LanguageManager.GetEnabledLanguages()
        for lang in enabledLanguages:
            # print(lang)
            self.logger.info(lang)


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

