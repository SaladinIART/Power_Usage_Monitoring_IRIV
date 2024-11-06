import asyncio
import minimalmodbus
import csv
import logging
from datetime import datetime
import time
import pymssql
import json
import psutil
from pathlib import Path

# Load configuration from config.json
with open('config.json') as config_file:
    config = json.load(config_file)

# Set up logging
logging.basicConfig(filename='rx380_logger.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

class RX380:
    def __init__(self, port=config["modbus"]["port"], slave_address=config["modbus"]["slave_address"]):
        self.instrument = minimalmodbus.Instrument(port, slave_address)
        self.instrument.serial.baudrate = 19200
        self.instrument.serial.bytesize = 8
        self.instrument.serial.parity = minimalmodbus.serial.PARITY_EVEN
        self.instrument.serial.stopbits = 1
        self.instrument.serial.timeout = 1
        self.instrument.mode = minimalmodbus.MODE_RTU

    async def read_data(self):
        try:
            data = {}
            # Example data fetching; add your actual data reading logic
            data['voltage_l1'] = await asyncio.to_thread(self.instrument.read_register, 4034, 0.1)
            data['voltage_l2'] = await asyncio.to_thread(self.instrument.read_register, 4036, 0.1)
            # Add other fields as needed
            return data
        except Exception as e:
            logging.error(f"Error reading data from RX380: {e}")
            return None

class DataManager:
    def __init__(self):
        self.db_config = config["database"]
        self.conn = None
        self.backup_file = 'unsaved_data.json'
        self.save_csv = config["save_csv"]
        self.retry_attempts = config["retry_attempts"]

    async def connect(self):
        if not self.conn:
            self.conn = await asyncio.to_thread(pymssql.connect, **self.db_config)

    async def close(self):
        if self.conn:
            await asyncio.to_thread(self.conn.close)
            self.conn = None

    async def save_to_sql(self, data):
        await self.connect()
        insert_query = """
        INSERT INTO Office_Readings 
           (Timestamp, VoltageL1_v, VoltageL2_v)
        VALUES (%s, %s, %s)
        """
        delay = 1  # Initial backoff time in seconds
        for attempt in range(self.retry_attempts):
            try:
                cursor = self.conn.cursor()
                values = (data['timestamp'], data['voltage_l1'], data['voltage_l2'])
                await asyncio.to_thread(cursor.execute, insert_query, values)
                await asyncio.to_thread(self.conn.commit)
                logging.info("Data inserted successfully into SQL Server.")
                return
            except Exception as e:
                logging.error(f"Error inserting data into SQL (attempt {attempt+1}/{self.retry_attempts}): {e}")
                time.sleep(delay)
                delay *= 2  # Exponential backoff
        # If all attempts fail, back up the data
        self.backup_unsaved_data([data])

    def backup_unsaved_data(self, data_buffer):
        try:
            with open(self.backup_file, 'a') as backup:
                json.dump(data_buffer, backup)
                backup.write("\n")
            logging.warning(f"Data backed up to {self.backup_file}")
        except Exception as e:
            logging.error(f"Failed to backup unsaved data: {e}")

    async def save_to_csv(self, data):
        if not self.save_csv:
            return
        folder_path = Path(config["csv"]["folder_path"])
        folder_path.mkdir(parents=True, exist_ok=True)
        filename = folder_path / f"rx380_data_{datetime.now().strftime('%Y-%m-%d')}.csv"
        file_exists = filename.is_file()
        try:
            async with asyncio.Lock():
                with open(filename, 'a', newline='') as csvfile:
                    fieldnames = ['timestamp'] + list(data.keys())
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    if not file_exists:
                        writer.writeheader()
                    writer.writerow(data)
            logging.info(f"Data saved to CSV: {filename}")
        except Exception as e:
            logging.error(f"Error saving data to CSV: {e}")

def log_system_metrics():
    cpu = psutil.cpu_percent()
    memory = psutil.virtual_memory().percent
    disk = psutil.disk_usage('/').percent
    logging.info(f"System Metrics - CPU: {cpu}%, Memory: {memory}%, Disk: {disk}%")

async def main():
    rx380 = RX380()
    data_manager = DataManager()
    logging.info("Starting RX380 data logging")

    try:
        while True:
            # Monitor system resources
            log_system_metrics()
            # Read data
            data = await rx380.read_data()
            if data:
                # Check if the current minute is at a 10-minute mark
                current_minute = datetime.now().minute
                if current_minute % 10 == 0:
                    data['timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    await data_manager.save_to_sql(data)
                    await data_manager.save_to_csv(data)
                    logging.info("Data saved at standardized 10-minute interval")

            # Adjust the reading interval if system load is high
            if psutil.cpu_percent() > 90:
                await asyncio.sleep(config["reading_interval_high"])
            else:
                await asyncio.sleep(config["reading_interval_normal"])

    except asyncio.CancelledError:
        pass
    except KeyboardInterrupt:
        logging.info("Program interrupted by user. Shutting down...")
    finally:
        await data_manager.close()
        logging.info("RX380 data logging shut down")

if __name__ == "__main__":
    asyncio.run(main())