import asyncio
import minimalmodbus
import csv
import logging
from datetime import datetime
import pymssql
import json
from pathlib import Path

# Set up logging
logging.basicConfig(filename='rx380_logger.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

class RX380:
    def __init__(self, port='/dev/ttyUSB0', slave_address=1):
        self.instrument = minimalmodbus.Instrument(port, slave_address)
        self.instrument.serial.baudrate = 19200
        self.instrument.serial.bytesize = 8
        self.instrument.serial.parity = minimalmodbus.serial.PARITY_EVEN
        self.instrument.serial.stopbits = 1
        self.instrument.serial.timeout = 1
        self.instrument.mode = minimalmodbus.MODE_RTU

    async def read_data(self):
        # Example data fetching method
        data = {}
        # Populate `data` with real readings (this should be customized as needed)
        data['voltage_l1'] = await asyncio.to_thread(self.instrument.read_register, 4034, 0.1)
        data['voltage_l2'] = await asyncio.to_thread(self.instrument.read_register, 4036, 0.1)
        # Add other fields based on your project requirements
        return data

class DataManager:
    def __init__(self, save_csv=False):
        self.db_config = {
            'server': '192.168.0.226',
            'database': 'Power_Usage_Alumac',
            'user': 'sa',
            'password': 'password'
        }
        self.retry_attempts = 3
        self.backup_file = 'unsaved_data.json'
        self.save_csv = save_csv  # Control whether to save to CSV

    async def save_to_sql(self, data):
        insert_query = """
        INSERT INTO Office_Readings 
           (Timestamp, VoltageL1_v, VoltageL2_v, VoltageL3_v, VoltageL12_v, VoltageL23_v, VoltageL31_v,
            VoltageL12_maxv, VoltageL23_maxv, VoltageL31_maxv, VoltageL12_minv, VoltageL23_minv, VoltageL31_minv,
            CurrentL1_I, CurrentL2_I, CurrentL3_I, CurrentLn_I,
            TotalRealPower_kWh, TotalApparentPower_kWh, TotalReactivePower_kWh, TotalPowerFactor_kWh, Frequency,
            TotalRealEnergy, TotalReactiveEnergy, TotalApparentEnergy)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        try:
            conn = await asyncio.to_thread(pymssql.connect, **self.db_config)
            cursor = conn.cursor()
            values = (
                data['timestamp'], data['voltage_l1'], data['voltage_l2'], data['voltage_l3'],
                data['voltage_l12'], data['voltage_l23'], data['voltage_l31'],
                data['voltage_l12_max'], data['voltage_l23_max'], data['voltage_l31_max'],
                data['voltage_l12_min'], data['voltage_l23_min'], data['voltage_l31_min'],
                data['current_l1'], data['current_l2'], data['current_l3'], data['current_ln'],
                data['total_real_power'] / 1000, data['total_apparent_power'] / 1000, data['total_reactive_power'] / 1000,
                data['total_power_factor'], data['frequency'],
                data['total_real_energy'], data['total_reactive_energy'], data['total_apparent_energy']
            )
            await asyncio.to_thread(cursor.execute, insert_query, values)
            await asyncio.to_thread(conn.commit)
            logging.info("Data inserted successfully into SQL Server.")
        except Exception as e:
            logging.error(f"Error inserting data into SQL Server: {e}")
        finally:
            if 'cursor' in locals():
                await asyncio.to_thread(cursor.close)
            if 'conn' in locals():
                await asyncio.to_thread(conn.close)

    async def save_to_csv(self, data, folder_path=None):
        if not self.save_csv:
            return  # Skip CSV saving if disabled
        if folder_path is None:
            folder_path = Path.home() / "Desktop" / "PUA_Office" / "PUA" / "rx380_daily_logs"
        folder_path.mkdir(parents=True, exist_ok=True)
        filename = folder_path / f"rx380_data_{datetime.now().strftime('%Y-%m-%d')}.csv"
        file_exists = filename.is_file()
        
        async with asyncio.Lock():
            with open(filename, 'a', newline='') as csvfile:
                fieldnames = ['timestamp'] + list(data.keys())
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                if not file_exists:
                    writer.writeheader()
                writer.writerow(data)
        
        logging.info(f"Data saved to CSV file: {filename}")

async def main():
    rx380 = RX380(slave_address=1)
    data_manager = DataManager(save_csv=True)  # Enable or disable CSV saving here
    
    logging.info("Starting RX380 data logging")
    print("RX380 data logging started.")
    
    try:
        while True:
            data = await rx380.read_data()
            if data:
                # Check if current time is on a 10-minute interval (e.g., :00, :10, :20)
                current_minute = datetime.now().minute
                if current_minute % 10 == 0:
                    # Delay to the next interval if program started at an odd time
                    while datetime.now().minute % 10 != 0:
                        await asyncio.sleep(1)  # Check every second until the next 10-minute mark
                    
                    data['timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    
                    # Save data to SQL and optionally to CSV
                    await data_manager.save_to_sql(data)
                    await data_manager.save_to_csv(data)
                    logging.info(f"Data saved at standardized 10-minute interval")

            await asyncio.sleep(10)  # Read data every 10 seconds

    except asyncio.CancelledError:
        pass
    except KeyboardInterrupt:
        print("Program interrupted by user. Shutting down...")
    finally:
        logging.info("Shutting down RX380 data logging")
        print("RX380 data logging shut down.")

if __name__ == "__main__":
    asyncio.run(main())