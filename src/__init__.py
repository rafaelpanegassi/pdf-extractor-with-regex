from configs.tools.queue import HTMLSQSListener
from extractor_text_pdf import PDFTextExtractor
from table_pdf_extractor import PDFTableExtractor
from configs.rules.notas import rules_dict

if __name__ == "__main__":
    #HTMLSQSListener().check_messages()
    #PDFTextExtractor("corretora_jornada_de_dados (1).pdf").start()
    configs = rules_dict['jornada']
    PDFTableExtractor("corretora_jornada_de_dados (1).pdf", configs=configs).start()