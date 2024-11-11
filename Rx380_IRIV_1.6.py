import asyncio
import minimalmodbus
import logging
import pymssql
import json
from datetime import datetime

# Load configuration from config.json
try:
    with open('config.json') as config_file:
        config = json.load(config_file)
except Exception as e:
    print(f"Failed to load configuration: {e}")
    exit(1)

# Set up logging
logging.basicConfig(
    filename='rx380_logger_v1.6.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

class RX380:
    """Class to handle Modbus communication with RX380 device."""
    
    def __init__(self, port='/dev/ttyACM0', slave_address=1):
        try:
            self.instrument = minimalmodbus.Instrument(port, slave_address)
            self.setup_instrument()
            logging.info("RX380 initialized successfully.")
        except Exception as e:
            logging.error(f"Failed to initialize RX380: {e}")

    def setup_instrument(self):
        """Configure the Modbus instrument settings."""
        try:
            self.instrument.serial.baudrate = 19200
            self.instrument.serial.bytesize = 8
            self.instrument.serial.parity = minimalmodbus.serial.PARITY_EVEN
            self.instrument.serial.stopbits = 1
            self.instrument.serial.timeout = 1
            self.instrument.mode = minimalmodbus.MODE_RTU
            logging.info("Instrument configured successfully.")
        except Exception as e:
            logging.error(f"Error setting up instrument: {e}")

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
            # Example readings
            data['voltage_l1'] = await self.read_scaled_value(4034, 0.1)
            data['current_l1'] = await self.read_scaled_value(4020, 0.001)
            # Add other necessary data points similarly

            return data
        except Exception as e:
            logging.error(f"Error reading data: {e}")
            return None

class DataManager:
    """Class to handle data storage in SQL."""
    
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
           (Timestamp, VoltageL1_v, CurrentL1_I) 
        VALUES (%s, %s, %s)
        """
        try:
            conn = await asyncio.to_thread(pymssql.connect, **self.db_config)
            cursor = conn.cursor()
            for data in data_buffer:
                values = (data['timestamp'], data['voltage_l1'], data['current_l1'])
                await asyncio.to_thread(cursor.execute, insert_query, values)
            await asyncio.to_thread(conn.commit)
            logging.info(f"Data inserted successfully into SQL Server! ({len(data_buffer)} records)")
        except pymssql.OperationalError as e:
            logging.error(f"Operational error in SQL connection: {e}")
        except Exception as e:
            logging.error(f"Error saving data to SQL Server: {e}")
        finally:
            if conn:
                conn.close()

async def main():
    rx380 = RX380('/dev/ttyACM0', 1)
    data_manager = DataManager()
    
    while True:
        data = await rx380.read_data()
        if data:
            data['timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            await data_manager.save_to_sql([data])
        await asyncio.sleep(10)  # Adjust as needed for your sampling rate

if __name__ == "__main__":
    asyncio.run(main())