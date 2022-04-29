import pandas as pd
from jdict import jdict
from datetime import date, timedelta

RANGE_CONFIG = jdict(
    single = 1,
    sum_recieved = ("Total Sum recieved for 1 person", "Period (days)"),
    exchange = ("Sum between 2 people", "Period (days)")
)
LIMIT_CONFIG = jdict(
    single = ("Single transfer","Limit"),
    sum_recieved = ("Total Sum recieved for 1 person", "Limit"),
    exchange = ("Sum between 2 people", "Limit")
)
      
""" Data handling """
    
def past_date(days_ago: int) -> str:
    """ Returns date in str format, for days_ago = 0 returns yesterday """
    return str(date.today()-timedelta(days_ago+1))
    
def get_recent_data(data: pd.DataFrame, time_range: int) -> pd.DataFrame:
    """ Returns data from last <time_range> days """
    return data[data["Date"] > past_date(time_range)]
    
def refresh_data(old: pd.DataFrame, new: pd.DataFrame, days_ago: int) -> pd.DataFrame:
    """ Formats new data and adds it to the old datafreme """
    new["Date"] = past_date(days_ago)
    return pd.concat([old,new], ignore_index = True)
    
def extract_rules(rules: pd.DataFrame) -> jdict:
    return jdict(
        time_ranges = extract_rules_by_config(rules, RANGE_CONFIG),
        limits = extract_rules_by_config(rules, LIMIT_CONFIG)
    )
    
def extract_rules_by_config(df: pd.DataFrame, schema: jdict) -> jdict:
    """ Reads time range or limit values for each usecase """
    rules = jdict()
    for key in schema.keys():
        try:
            usecase, target = schema[key]
            rules[key] = df[df["Usecase"] == usecase][target].item()
        # to do - different types of exceptions
        except TypeError:
            rules[key] = schema[key]
        
    return rules
    
""" Violations checks """
        
def get_pairs(data: pd.DataFrame) -> pd.DataFrame:
    """ Returns a df containing unique source-destination pairs """
    pair_data = data.assign(between=data["Source"]+","+data["Destination"])
    pair_data = pair_data[["between","Sum transfered"]]
        
    # let's sort the pairs to see (x,y) the same as (y,x) - separate and test it
    pair_data["between"] = pair_data["between"].apply(lambda x: ", ".join(sorted(x.split(","))), 1)
    return pair_data
    
def group_and_check(data: pd.DataFrame, limit: int, target: str) -> pd.DataFrame:
    """ Groups sums all transfers with the same <target> and checks for limit violations """
    incomes = data.groupby(target, as_index = False).sum()
    return incomes[incomes["Sum transfered"] > limit]
    
def check_single(data: pd.DataFrame, limit: int) -> pd.DataFrame:
    data = data[["Source","Destination","Sum transfered"]]
    return data[data["Sum transfered"] > limit]
    
def check_sum(data: pd.DataFrame, limit: int)-> pd.DataFrame:
    data = data[["Destination","Sum transfered"]]
    return group_and_check(data,limit,"Destination")
    
def check_exchanged(data: pd.DataFrame, limit: int)-> pd.DataFrame:
    data = get_pairs(data)
    return group_and_check(data,limit,"between")
    
SCREENING_FUNCTIONS = dict(
        single = check_single,
        sum_recieved = check_sum,
        exchange = check_exchanged
    )
    
def screen_data(screening_type: str, data: pd.DataFrame, limit: int) -> pd.DataFrame:
    ''' maps usecase to a search function and runs the function '''
    check = SCREENING_FUNCTIONS[screening_type]
    return check(data, limit)
    
def detect_limit_violations(data: pd.DataFrame, rules: jdict, violations_type: str):
    return screen_data(
        violations_type, 
        get_recent_data(
            data,
            rules.time_ranges[violations_type]
        ), 
        rules.limits[violations_type]
    )
    
def build_results(violations_flags: list) -> jdict:
    ''' Returns result of the search '''
    return jdict(
        result = "Alerts found", 
        detected = jdict(zip(["single","sum_recieved","exchange"],violations_flags))
    ) if any(violations_flags) else jdict(result = "No alerts")
    