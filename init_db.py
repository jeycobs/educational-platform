import asyncio
import sys
from pathlib import Path

current_dir = Path(__file__).parent
if str(current_dir) not in sys.path:
    sys.path.append(str(current_dir))

from db import Base, engine 
from fill_test_data import fill_with_sample_data

async def init_database():
    try:
        async with engine.begin() as conn:
            print("Dropping all existing tables...")
            await conn.run_sync(Base.metadata.drop_all)
            print("Creating all tables based on current models...")
            await conn.run_sync(Base.metadata.create_all)
        
        print('База данных успешно инициализирована (таблицы пересозданы)!')
        
        
        print("Attempting to fill database with sample data...")
        await fill_with_sample_data() 
        print('Тестовые данные (если были определены) должны быть добавлены!')
        
    except Exception as e:
        print(f'Ошибка инициализации базы данных: {e}')
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        await engine.dispose()


if __name__ == "__main__":
    print("Running database initialization script...")
    asyncio.run(init_database())