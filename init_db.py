# init_db.py
import asyncio
import sys
from pathlib import Path

# Добавляем текущую директорию в путь, чтобы найти db.py и fill_test_data.py
# Это может быть не нужно, если вы запускаете скрипт из корневой директории проекта,
# но не помешает для надежности.
current_dir = Path(__file__).parent
if str(current_dir) not in sys.path:
    sys.path.append(str(current_dir))

# Теперь импортируем после изменения пути
from db import Base, engine 
from fill_test_data import fill_with_sample_data # Убедитесь, что функция так называется

async def init_database(): # Переименовал для ясности
    """Инициализация базы данных: удаление старых таблиц и создание новых."""
    try:
        async with engine.begin() as conn:
            print("Dropping all existing tables...")
            await conn.run_sync(Base.metadata.drop_all)
            print("Creating all tables based on current models...")
            await conn.run_sync(Base.metadata.create_all)
        
        print('✅ База данных успешно инициализирована (таблицы пересозданы)!')
        
        # Заполняем тестовыми данными
        # Убрал интерактивный выбор, чтобы упростить автоматизацию.
        # Если нужен интерактивный выбор, верните его.
        # choice = input('Заполнить базу тестовыми данными? (y/N): ')
        # if choice.lower() in ['y', 'yes', 'д', 'да']:
        print("Attempting to fill database with sample data...")
        await fill_with_sample_data() # Вызываем функцию напрямую
        print('✅ Тестовые данные (если были определены) должны быть добавлены!')
        
    except Exception as e:
        print(f'❌ Ошибка инициализации базы данных: {e}')
        import traceback
        traceback.print_exc() # Для более детальной информации об ошибке
        sys.exit(1)
    finally:
        await engine.dispose() # Важно закрыть соединение с движком

if __name__ == "__main__":
    print("Running database initialization script...")
    asyncio.run(init_database())