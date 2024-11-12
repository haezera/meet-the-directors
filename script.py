import pandas as pd
import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import time
from collections import defaultdict # average leetcode moment
import signal 
from contextlib import contextmanager
import datetime 
from maps import maps

class TimeoutException(Exception):
    pass

@contextmanager
def timeout(seconds):
    def signal_handler(signum, frame):
        raise TimeoutException("Timed out!")
    
    signal.signal(signal.SIGALRM, signal_handler)
    signal.alarm(seconds)
    
    try:
        yield
    finally:
        signal.alarm(0)

def scrape_when2meet(url, output_path, timeout_seconds=30):
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    driver = webdriver.Chrome(options=options)
    
    try:
        with timeout(timeout_seconds):
            driver.get(url)
            time.sleep(5)
            
            # Get required data
            people_names = driver.execute_script("return PeopleNames")
            people_ids = driver.execute_script("return PeopleIDs")
            available_at_slot = driver.execute_script("return AvailableAtSlot")
            time_of_slot = driver.execute_script("return TimeOfSlot")
            
            if not people_names or len(people_names) == 0:
                raise Exception(f"No participants found for {output_path}")
            
            # Build CSV
            result = ["Time," + ",".join(people_names)]
            
            for i in range(len(available_at_slot)):
                slot = driver.execute_script(
                    f'return document.evaluate(\'string(//div[@id="GroupTime{time_of_slot[i]}"]/@onmouseover)\', document, null, XPathResult.STRING_TYPE, null).stringValue'
                )
                slot = slot.split('"')[1]
                
                availability = [
                    "1" if pid in available_at_slot[i] else "0"
                    for pid in people_ids
                ]
                
                result.append(slot + "," + ",".join(availability))
            
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write("\n".join(result))
                
    except TimeoutException:
        print(f"Operation timed out after {timeout_seconds} seconds")
        raise
    except Exception as e:
        print(f"Error: {str(e)}")
        raise
    finally:
        driver.quit()

def consolidate_time_ranges(df: pd.DataFrame, media_directors: list[str]):
    # Extract weekday and convert time portion to datetime
    df['weekday'] = df['Time'].str.split().str[0]
    df['hour time'] = df['Time'].str.split().str[1] + " " + df['Time'].str.split().str[2]
    df['datetime'] = pd.to_datetime(df['hour time'], format='mixed')
    
    # Process each weekday separately
    result = []
    for weekday in df['weekday'].unique():
        day_df = df[df['weekday'] == weekday].sort_values('datetime')
        
        start_time = None
        prev_time = None
        start_row = None
        
        for _, row in day_df.iterrows():
            current_time = row['datetime']
            
            if start_time is None:
                start_time = current_time
                prev_time = current_time
                start_row = row
                continue
                
            time_diff = (current_time - prev_time).total_seconds() / 60
            
            if time_diff != 15:
                if prev_time > start_time:
                    # Get directors available for all times in this range
                    range_df = day_df[
                        (day_df['datetime'] >= start_time) & 
                        (day_df['datetime'] <= prev_time)
                    ]
                    # Find directors that have all 1s in this range
                    available_directors = [
                        director for director in media_directors 
                        if all(range_df[director] == 1)
                    ]
                    result.append({
                        'Time': f"{weekday} {start_time.strftime('%I:%M:%S %p')} - {prev_time.strftime('%I:%M:%S %p')}",
                        'Available Directors': ', '.join(available_directors)
                    })
                start_time = current_time
                prev_time = current_time
                start_row = row
            else:
                prev_time = current_time
        
        # Add the last range for this weekday if it exists
        if prev_time > start_time:
            range_df = day_df[
                (day_df['datetime'] >= start_time) & 
                (day_df['datetime'] <= prev_time)
            ]
            available_directors = [
                director for director in media_directors 
                if all(range_df[director] == 1)
            ]
            result.append({
                'Time': f"{weekday} {start_time.strftime('%I:%M:%S %p')} - {prev_time.strftime('%I:%M:%S %p')}",
                'Available Directors': ', '.join(available_directors)
            })
    
    return pd.DataFrame(result)

def find_possible_schedules(override: bool):
    """
    Okay this one's actually written by me (Hae) so you can ask me.
    """

    def fetch_directors_df_and_name(port_name: str):
        """
        Fetches the directors availabilities of a given port and their names.
        """
        directors = pd.read_csv(f"./data/{port_name}.csv")
        director_names = list(directors.columns)
        director_names.remove("Time")

        return directors, director_names
    
    media_path = f"./data/media.csv"
    if not os.path.exists(media_path) or override:
        try:
            scrape_when2meet(maps["media"], media_path)
        except Exception as e:
            print("You bums didn't do your when2meets properly...")
            return

        
    schedule_results = defaultdict(defaultdict)
    
    for port in maps.keys():
        port_results = defaultdict(pd.DataFrame)

        # we don't need to do us LOL
        if port == "media": continue
        data_path = f"./data/{port}.csv"

        # if we haven't scraped the csv yet, then save (will take a bit).
        if not os.path.exists(data_path) or override:
            try:
                scrape_when2meet(maps[port], data_path)
            except Exception as e:
                print(f"Processing when2meet for {port} has failed. Please manually check.")
                continue

        

        port_df, port_directors = fetch_directors_df_and_name(port)
        media_df, media_directors = fetch_directors_df_and_name("media")

        merged = port_df.merge(media_df, on="Time")

        # This filters the times and weekdays where all the participating directors
        # are available.
        merged = merged[merged[port_directors].all(axis=1)]

        # Ensure atleast 2 media directors are able to make it.
        merged = merged[merged[media_directors].sum(axis=1) >= 2]
        merged["Weekday"] = merged["Time"].apply(lambda x: x.split(" ")[0])
        merged["HMS Time"] = merged["Time"].apply(lambda x: x.split(" ")[1])
        merged["AM/PM"] = merged["Time"].apply(lambda x: x.split(" ")[2])

        if merged.empty:
            port_results[name] = None
            continue

        for name, group in merged.groupby("Weekday"):
            group = group.sort_values(["AM/PM", "HMS Time"])
            port_results[name] = consolidate_time_ranges(group, media_directors)

        schedule_results[port] = port_results 

    return schedule_results

def pretty_print_port(port: str, results: defaultdict):
    print("")
    print(f"{port}")
    port_schedule = results[port]
    if port_schedule is None:
        print(f"{port} does not have a overlapping time.")
        print("")
        return
    for day in ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]:
        if day not in port_schedule.keys(): continue 

        print(f"Availabilities on {day}")
        times = port_schedule[day][["Time", "Available Directors"]].values
        for time in times:
            print(f"\t{time[0]}\t{time[1]}")

results = find_possible_schedules(False)
for port in results.keys():
    print("")
    pretty_print_port(port, results)