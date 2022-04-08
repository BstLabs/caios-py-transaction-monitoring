import pandas as pd
from jdict import jdict
from datetime import date, timedelta
from caios.abc.mapping import Mapping
from caios.abc.mutable_mapping import MutableMapping
from domain_logic import past_date, refresh_data
from collections.abc import Callable

def get_types() -> dict:
    return {
        "single" : "single",
        "sum_recieved" : "sum_recieved",
        "exchange" : "exchange"
    }

def format_process(func: Callable, violation_type: str) -> Callable:
    """ Wraps the search function for parallel call """
    async def process():
        result = func(violation_type)
        return result
    return process

def format_transaction_data(spreadsheet: Mapping, time_range: int) -> pd.DataFrame:
    """ Returns a data frame frame with transactions from last WINDOW LENGTH days"""
    data = pd.DataFrame(columns = ["Source","Destination","Sum transfered","Date"])
        
    for days_ago in range(time_range):
        sheet = format_ss_name(days_ago)
            
        if sheet in spreadsheet:
            data = refresh_data(data,spreadsheet[sheet],days_ago)
                
    return data

def format_ss_name(days_ago: int) -> str:
    """ Changes date format to the one used in the TransactionMonitoring spreadsheet """
    year, month, day =  past_date(days_ago).split("-")
    return ".".join([day,month,year])
    
def format_alert_filename(violations_type: str):
    return f'{past_date(0)}_{violations_type}'
    
def format_alert(data: pd.DataFrame)-> str:
    ''' Changes alert format to string '''
    data = str(data.values)
    data = data.replace("[","")
    data = data.replace("]","")
    data = data.replace("'","")
    return data
 
def format_chunk(key: str, alert: str):
    ''' Formats alert string for a specific usecase '''
    names = {
        "single":"single transfer (source, destination, amount)",
        "sum_recieved":"total sum recieved for one person",
        "exchange":"sum exchanged between two people"
    }
    return f"Alerts for {names[key]}: {alert}"
    
def format_chunks(alerts: MutableMapping, violations: jdict) -> list:
    ''' Returns a list of lines to be sent in an email '''
    chunks = [format_chunk(key, alerts[format_alert_filename(key)]) for key in violations.keys() if violations[key]]
    return [f'Anomalies detected on {past_date(0)}: \n'] + chunks
    
def format_html(chunks: list)-> str:
    ''' Puts each chunk in a separate html parahgraph '''
    chunks = [chunk+"</p>" for chunk in chunks]
    return '<p> <p> '+ '<p>'.join(chunks) +' </p>'
    
def format_email(alerts: MutableMapping, violations: jdict, recipient: str) -> jdict:
    ''' Returns jdict with formatted details of an email '''
    chunks = format_chunks(alerts, violations)
    message = "\n".join(chunks)
    return jdict(
        message = message,
        recipients = [recipient],
        subject = "Transaction moniotring alerts",
        body_text = message,
        body_html = format_html(chunks)
    )