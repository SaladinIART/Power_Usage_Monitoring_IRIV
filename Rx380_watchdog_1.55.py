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
    """Class to handle Modbus communication with RX380 device."""
    def __init__(self, port='/dev/ttyUSB0', slave_address=1):
        self.instrument = minimalmodbus.Instrument(port, slave_address)
        self.setup_instrument()

    def setup_instrument(self):
        """Configure the Modbus instrument settings."""
        self.instrument.serial.baudrate = 19200
        self.instrument.serial.bytesize = 8
        self.instrument.serial.parity = minimalmodbus.serial.PARITY_EVEN
        self.instrument.serial.stopbits = 1
        self.instrument.serial.timeout = 1
        self.instrument.mode = minimalmodbus.MODE_RTU

    async def read_scaled_value(self, register_address, scale_factor):
        """Read and scale values from the Modbus registers."""
        try:
            raw_value = await asyncio.to_thread(
                self.instrument.read_registers, register_address, 2, functioncode=4
            )
            value = (raw_value[0] << 16 | raw_value[1]) * scale_factor
            return value
        except Exception as e:
            logging.error(f"Error reading scaled value from register {register_address}: {e}")
            return None

    async def read_register(self, register_address, number_of_decimals, signed):
        """Read a single register value."""
        try:
            return await asyncio.to_thread(
                self.instrument.read_register, register_address, number_of_decimals, signed=signed, functioncode=4
            )
        except Exception as e:
            logging.error(f"Error reading register {register_address}: {e}")
            return None

    async def read_data(self):
        """Read all necessary data from RX380."""
        data = {}
        try:
            # Read voltages
            data['voltage_l1'] = await self.read_scaled_value(4034, 0.1)  # V
            data['voltage_l2'] = await self.read_scaled_value(4036, 0.1)  # V
            data['voltage_l3'] = await self.read_scaled_value(4038, 0.1)  # V
            data['voltage_l12'] = await self.read_scaled_value(4028, 0.1)  # V
            data['voltage_l23'] = await self.read_scaled_value(4030, 0.1)  # V
            data['voltage_l31'] = await self.read_scaled_value(4032, 0.1)  # V

            # Read max voltages
            data['voltage_l12_max'] = await self.read_scaled_value(4124, 0.1)  # V
            data['voltage_l23_max'] = await self.read_scaled_value(4128, 0.1)  # V
            data['voltage_l31_max'] = await self.read_scaled_value(4132, 0.1)  # V

            # Read min voltages
            data['voltage_l12_min'] = await self.read_scaled_value(4212, 0.1)  # V
            data['voltage_l23_min'] = await self.read_scaled_value(4216, 0.1)  # V
            data['voltage_l31_min'] = await self.read_scaled_value(4220, 0.1)  # V

            # Read current
            data['current_l1'] = await self.read_scaled_value(4020, 0.001)  # A
            data['current_l2'] = await self.read_scaled_value(4022, 0.001)  # A
            data['current_l3'] = await self.read_scaled_value(4024, 0.001)  # A
            data['current_ln'] = await self.read_scaled_value(4026, 0.001)  # A

            # Read power
            data['total_real_power'] = await self.read_scaled_value(4012, 1)  # W
            data['total_apparent_power'] = await self.read_scaled_value(4014, 1)  # VA
            data['total_reactive_power'] = await self.read_scaled_value(4016, 1)  # VAR

            # Read power factor and frequency
            data['total_power_factor'] = await self.read_register(4018, 3, True)
            data['frequency'] = await self.read_register(4019, 2, False)  # Hz

            # Read energy
            data['total_real_energy'] = await self.read_scaled_value(4002, 1)  # kWh
            data['total_reactive_energy'] = await self.read_scaled_value(4010, 1)  # kVARh
            data['total_apparent_energy'] = await self.read_scaled_value(4006, 1)  # kVAh

            return data
        except Exception as e:
            logging.error(f"Error reading data: {e}")
            return None

class DataManager:
    """Class to handle data storage in SQL and CSV."""
    def __init__(self):
        self.db_config = {
            'server': '192.168.0.226',
            'database': 'Power_Usage_Alumac',
            'user': 'sa',
            'password': 'password'
        }

    async def save_to_sql(self, data_buffer):
        """Save data to SQL Server with error handling."""
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
            for data in data_buffer:
                values = (
                    data['timestamp'],
                    data['voltage_l1'], data['voltage_l2'], data['voltage_l3'],
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
            logging.info(f"Data inserted successfully into SQL Server! ({len(data_buffer)} records)")
        except Exception as e:
            logging.error(f"Error inserting data into SQL Server: {e}")
            if 'conn' in locals():
                await asyncio.to_thread(conn.rollback)
        finally:
            if 'cursor' in locals():
                await asyncio.to_thread(cursor.close)
            if 'conn' in locals():
                await asyncio.to_thread(conn.close)

def get_filename(extension):
    """Generate a filename based on the current date."""
    today = datetime.now().strftime("%Y-%m-%d")
    return f"rx380_data_{today}.{extension}"

async def save_to_csv(data_buffer, folder_path=None):
    """Save data to a CSV file."""
    if folder_path is None:
        folder_path = Path.home() / "Desktop" / "PUA_Office" / "PUA" / "rx380_daily_logs"
    folder_path = Path(folder_path)
    folder_path.mkdir(parents=True, exist_ok=True)
    
    filename = folder_path / get_filename("csv")
    file_exists = filename.is_file()
    
    async with asyncio.Lock():
        with open(filename, 'a', newline='') as csvfile:
            fieldnames = ['timestamp'] + list(data_buffer[0].keys())
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            if not file_exists:
                writer.writeheader()
            
            for data in data_buffer:
                writer.writerow(data)
    
    logging.info(f"Data saved to CSV file: {filename}")

async def main():
    """Main function to handle data reading, saving, and logging."""
    rx380 = RX380(slave_address=1)
    data_manager = DataManager()
    
    logging.info("Starting RX380 data logging")
    print("RX380 data logging started.")
    
    data_buffer = []
    display_counter = 0
    
    try:
        while True:
            try:
                data = await rx380.read_data()
                if data:
                    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    data['timestamp'] = timestamp
                    data_buffer.append(data)
                    logging.info("Data read successfully")
                    
                    # Display output on terminal every 2 minutes
                    display_counter += 1
                    if display_counter >= 12:  # 12 * 10 seconds = 2 minutes
                        print(f"\nRX380 Readings at {timestamp}:")
                        print(f"Line Voltage (V): L12={data['voltage_l12']:.1f}, L23={data['voltage_l23']:.1f}, L31={data['voltage_l31']:.1f}")
                        print(f"Current (A): L1={data['current_l1']:.2f}, L2={data['current_l2']:.2f}, L3={data['current_l3']:.2f}")
                        print(f"Total Real Power: {data['total_real_power']} W")
                        print(f"Total Power Factor: {data['total_power_factor']:.3f}")
                        print(f"Frequency: {data['frequency']:.2f} Hz")
                        display_counter = 0
                    
                    # Save data to SQL every 1 minute
                    if len(data_buffer) >= 6:  # 6 * 10 seconds = 1 minute
                        await data_manager.save_to_sql(data_buffer)
                        data_buffer.clear()
                    
                    # Save data to CSV every 5 minutes
                    if len(data_buffer) >= 30:  # 30 * 10 seconds = 5 minutes
                        await save_to_csv(data_buffer)
                        data_buffer.clear()
                else:
                    logging.warning("Failed to read data")
            except Exception as e:
                logging.error(f"Error in main loop: {e}")
                print(f"Error: {e}")
            
            await asyncio.sleep(10)  # Read data every 10 seconds
    except asyncio.CancelledError:
        pass
    except KeyboardInterrupt:
        print("Program interrupted by user. Shutting down...")
    finally:
        # Save any remaining data in the buffer
        if data_buffer:
            await asyncio.gather(
                save_to_csv(data_buffer),
                data_manager.save_to_sql(data_buffer)
            )
        logging.info("Shutting down RX380 data logging")
        print("RX380 data logging shut down.")

if __name__ == "__main__":
    asyncio.run(main())