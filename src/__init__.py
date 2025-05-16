import time
from datetime import datetime

import schedule

from configs.tools.queue import HTMLSQSListener
from extractor_text_pdf import PDFTextExtractor
from table_pdf_extractor import PDFTableExtractor
from configs.rules.notas import rules_dict

from loguru import logger


def task_every_2_minutes():
    """
    Task to be executed every 2 minutes.
    """
    logger.info(f"Task executed every executed in {datetime.now()}")
    HTMLSQSListener().check_messages()
    
def schedule_tasks():
    """
    Schedule tasks to be executed at specific intervals.
    """
    schedule.every(20).seconds.do(task_every_2_minutes)
    logger.info("Scheduled tasks initialized.")
    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    schedule_tasks()
    #HTMLSQSListener().check_messages()
    #PDFTextExtractor("corretora_jornada_de_dados (1).pdf").start()
    #configs = rules_dict['jornada']
    #PDFTableExtractor("corretora_jornada_de_dados (1).pdf", configs=configs).start()