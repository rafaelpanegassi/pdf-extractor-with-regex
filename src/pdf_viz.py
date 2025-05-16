import os

import camelot
import matplotlib
import matplotlib.pyplot as plt
import pandas as pd
from loguru import logger

file_name = "corretora_jornada_de_dados (1)"
path = os.path.abspath(f"src/files/pdf/jornada/{file_name}.pdf")

tables = camelot.read_pdf(
    path, flavor="stream", table_areas=["72, 560, 492, 390"], strip_text=".\n"
)

camelot.plot(tables[0], kind="contour")

plt.show()

logger.info("PDF visualization complete.")
