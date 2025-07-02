import logging
import azure.functions as func
from .json_parser import main as parse_lineage

def main(blob: func.InputStream):
    parse_lineage(blob)