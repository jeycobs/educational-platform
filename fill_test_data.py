# fill_test_data.py
import asyncio
from datetime import datetime, timedelta
import random

# Аналогично init_db.py, обеспечим нахождение db.py
import sys
from pathlib import Path
current_dir = Path(__file__).parent
if str(current_dir) not in sys.path:
    sys.path.append(str(current_dir))

from db import SessionLocal, User, Course, Material, Activity # Импортируем модели SQLAlchemy
from passlib.context import CryptContext

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

async def fill_with_sample_data():
    """Заполнение базы расширенными тестовыми данными."""
    async with SessionLocal() as db:
        print("Creating sample users...")
        users_data = [
            {"name": "Иван Гений", "email": "ivan.genius@example.com", "role": "teacher", "is_active": True},
            {"name": "Анна Студентка", "email": "anna.student@example.com", "role": "student", "is_active": True},
            {"name": "Петр Пытливый", "email": "petr.curious@example.com", "role": "student", "is_active": True},
            {"name": "Мария Администратор", "email": "maria.admin@example.com", "role": "admin", "is_active": True},
            {"name": "Алексей Наставник", "email": "alex.mentor@example.com", "role": "teacher", "is_active": True},
            {"name": "Елена Умница", "email": "elena.smart@example.com", "role": "student", "is_active": False}, # Один неактивный
        ]
        
        created_users = []
        for user_data in users_data:
            user = User(
                name=user_data["name"],
                email=user_data["email"],
                role=user_data["role"],
                password_hash=pwd_context.hash("testpassword"), # Единый пароль для всех тестовых
                is_active=user_data.get("is_active", True),
                created_at=datetime.utcnow() - timedelta(days=random.randint(10, 100)) # Разное время создания
            )
            created_users.append(user)
        
        db.add_all(created_users)
        await db.commit()
        
        # Обновляем объекты пользователей, чтобы получить их ID
        # (SQLAlchemy ORM обычно делает это автоматически после commit, но явный refresh не повредит)
        refreshed_users = []
        for user_obj in created_users:
            await db.refresh(user_obj)
            refreshed_users.append(user_obj)
        
        teachers = [u for u in refreshed_users if u.role == "teacher" and u.is_active]
        if not teachers:
            print("Warning: No active teachers found to assign courses.")
            return # Не можем создать курсы без преподавателей

        print("Creating sample courses...")
        courses_data = [
            {
                "title": "Python для начинающих: Путь к автоматизации",
                "description": "Полный курс для тех, кто хочет изучить Python с нуля и применять его для автоматизации задач. Рассматриваются основы языка, работа с файлами, веб-скрапинг и многое другое.",
                "category": "Программирование", "level": "beginner", "teacher_id": teachers[0].id
            },
            {
                "title": "Глубокое погружение в Data Science",
                "description": "Продвинутый курс по анализу данных, машинному обучению и визуализации с использованием Python, Pandas, Scikit-learn и Matplotlib.",
                "category": "Аналитика", "level": "intermediate", "teacher_id": teachers[0].id
            },
            {
                "title": "Современная Веб-разработка с FastAPI и Vue.js",
                "description": "Научитесь создавать высокопроизводительные веб-приложения с асинхронным бэкендом на FastAPI и реактивным фронтендом на Vue.js.",
                "category": "Программирование", "level": "intermediate", "teacher_id": teachers[1].id if len(teachers) > 1 else teachers[0].id
            },
            {
                "title": "Основы Дискретной Математики для IT",
                "description": "Ключевые концепции дискретной математики, необходимые каждому разработчику: логика, теория множеств, графы, комбинаторика.",
                "category": "Математика", "level": "beginner", "teacher_id": teachers[0].id
            },
            {
                "title": "Цифровая трансформация: История и Будущее",
                "description": "Обзорный курс по истории развития цифровых технологий и их влиянию на общество, бизнес и повседневную жизнь.",
                "category": "Гуманитарные науки", "level": "beginner", "teacher_id": teachers[1].id if len(teachers) > 1 else teachers[0].id
            }
        ]
        
        created_courses = []
        for course_data in courses_data:
            course = Course(**course_data, created_at=datetime.utcnow() - timedelta(days=random.randint(5, 50)))
            created_courses.append(course)
        
        db.add_all(created_courses)
        await db.commit()

        refreshed_courses = []
        for course_obj in created_courses:
            await db.refresh(course_obj)
            refreshed_courses.append(course_obj)

        print("Creating sample materials...")
        all_materials = []
        material_titles_actions = {
            "video": ["Просмотр лекции", "Запись основных моментов"],
            "text": ["Чтение статьи", "Конспектирование"],
            "quiz": ["Прохождение теста", "Разбор ошибок"],
            "assignment": ["Выполнение задания", "Отправка решения"]
        }

        for i, course in enumerate(refreshed_courses):
            num_materials = random.randint(3, 7)
            for j in range(1, num_materials + 1):
                mat_type = random.choice(["video", "text", "quiz", "assignment"])
                material = Material(
                    course_id=course.id,
                    title=f"Урок {j}: {course.title.split(':')[0]} - {mat_type.capitalize()}",
                    content=f"Содержимое для урока {j} по теме '{course.title}'. Тип: {mat_type}. Это пример текста для материала.",
                    type=mat_type,
                    order_index=j,
                    created_at=datetime.utcnow() - timedelta(days=random.randint(1, course.created_at.day if course.created_at.day > 1 else 2))
                )
                all_materials.append(material)
        
        db.add_all(all_materials)
        await db.commit()

        refreshed_materials = []
        for mat_obj in all_materials:
            await db.refresh(mat_obj)
            refreshed_materials.append(mat_obj)
        
        print("Creating sample activities...")
        active_students = [u for u in refreshed_users if u.role == "student" and u.is_active]
        created_activities = []
        
        if active_students and refreshed_materials:
            for student in active_students:
                num_student_activities = random.randint(5, len(refreshed_materials) // 2 if len(refreshed_materials) > 10 else len(refreshed_materials))
                # Студент взаимодействует с подмножеством материалов
                materials_for_student = random.sample(refreshed_materials, min(num_student_activities * 2, len(refreshed_materials)))

                for material in materials_for_student[:num_student_activities]: # Взаимодействует не со всеми выбранными, а с частью
                    action = random.choice(material_titles_actions.get(material.type, ["view", "interact"]))
                    
                    activity = Activity(
                        user_id=student.id,
                        material_id=material.id,
                        action=action,
                        timestamp=datetime.utcnow() - timedelta(hours=random.randint(1, 720)), # Активность за последние 30 дней
                        duration=random.uniform(300, 3600) if action in ["view", "Просмотр лекции", "Чтение статьи"] else None, # 5-60 минут
                        score=round(random.uniform(0.6, 1.0), 2) if material.type == "quiz" and action == "Прохождение теста" else None,
                        meta={"device": random.choice(["desktop", "mobile"]), "ip_geo": random.choice(["RU", "US", "DE"])}
                    )
                    created_activities.append(activity)
            
            db.add_all(created_activities)
            await db.commit()
        else:
            print("No active students or materials to create activities for.")

        print(f"\n--- Sample Data Summary ---")
        print(f"  - Users: {len(refreshed_users)}")
        print(f"  - Courses: {len(refreshed_courses)}")
        print(f"  - Materials: {len(refreshed_materials)}")
        print(f"  - Activities: {len(created_activities)}")

if __name__ == "__main__":
    asyncio.run(fill_with_sample_data())