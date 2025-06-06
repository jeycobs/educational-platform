import asyncio
from datetime import datetime, timedelta
import random
import json

import sys
from pathlib import Path
current_dir = Path(__file__).parent
if str(current_dir) not in sys.path:
    sys.path.append(str(current_dir))

from db import SessionLocal, User, Course, Material, Activity
from passlib.context import CryptContext
from sqlalchemy import select, func

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
USER_PASSWORD = "testpassword"

async def fill_with_sample_data():
    async with SessionLocal() as db:
        print("Creating sample users...")
        users_data = [
            {"name": "Иван Гениев", "email": "ivan.genius@example.com", "role": "teacher", "is_active": True},
            {"name": "Анна Студенткина", "email": "anna.student@example.com", "role": "student", "is_active": True},
            {"name": "Петр Пытливый", "email": "petr.curious@example.com", "role": "student", "is_active": True},
            {"name": "Мария Администраторова", "email": "maria.admin@example.com", "role": "admin", "is_active": True},
            {"name": "Алексей Наставников", "email": "alex.mentor@example.com", "role": "teacher", "is_active": True},
            {"name": "Елена Отключенная", "email": "elena.inactive@example.com", "role": "student", "is_active": False},
        ]
        created_users_map = {}
        for user_data in users_data:
            existing_user_res = await db.execute(select(User).where(User.email == user_data["email"]))
            if existing_user_res.scalar_one_or_none():
                print(f"User with email {user_data['email']} already exists, loading.")
                existing_user = await db.scalar(select(User).filter_by(email=user_data["email"]))
                if existing_user: created_users_map[user_data["email"]] = existing_user
                continue
            user = User(name=user_data["name"], email=user_data["email"], role=user_data["role"],
                        password_hash=pwd_context.hash(USER_PASSWORD), is_active=user_data.get("is_active", True),
                        created_at=datetime.utcnow() - timedelta(days=random.randint(30, 365)))
            db.add(user)
            created_users_map[user.email] = user
        await db.commit()
        
        refreshed_users = []
        for email_key in created_users_map:
            fresh_user = await db.scalar(select(User).filter_by(email=email_key))
            if fresh_user: refreshed_users.append(fresh_user)
        
        active_teachers = [u for u in refreshed_users if u.role == "teacher" and u.is_active]
        if not active_teachers: print("Warning: No active teachers for courses.")

        print("Creating sample courses...")
        courses_data = [
            {"title": "Python для начинающих: Автоматизация и Скриптинг", "description": "Изучите основы Python, научитесь писать скрипты для автоматизации рутинных задач, работать с файлами и API. Идеально для новичков.", "category": "Программирование", "level": "beginner", "tags": "python,основы,автоматизация,скрипты", "teacher_id": active_teachers[0].id if active_teachers else None},
            {"title": "Data Science: от Pandas до Нейронных Сетей", "description": "Комплексный курс по анализу данных, машинному обучению и глубокому обучению. Практические проекты на реальных данных.", "category": "Аналитика", "level": "intermediate", "tags": "datascience,машинное обучение,pandas,python", "teacher_id": active_teachers[0].id if active_teachers else None},
            {"title": "Fullstack Веб-разработка: FastAPI и React", "description": "Создайте полноценное веб-приложение с нуля, используя FastAPI для бэкенда и React для интерактивного фронтенда.", "category": "Программирование", "level": "intermediate", "tags": "fastapi,react,fullstack,python,javascript,веб", "teacher_id": active_teachers[1].id if len(active_teachers) > 1 else (active_teachers[0].id if active_teachers else None)},
            {"title": "Основы Дискретной Математики для IT", "description": "Ключевые концепции дискретной математики, необходимые каждому разработчику: логика, теория множеств, графы, комбинаторика.", "category": "Математика", "level": "beginner", "tags": "математика,логика,графы,it", "teacher_id": active_teachers[0].id if active_teachers else None},
            {"title": "Цифровая трансформация: История и Будущее", "description": "Обзорный курс по истории развития цифровых технологий и их влиянию на общество, бизнес и повседневную жизнь.", "category": "Гуманитарные науки", "level": "beginner", "tags": "история,технологии,инновации,цифровизация", "teacher_id": active_teachers[1].id if len(active_teachers) > 1 else (active_teachers[0].id if active_teachers else None)}
        ]
        created_courses = []
        for course_data in courses_data:
            if course_data.get("teacher_id") is None and active_teachers: course_data["teacher_id"] = active_teachers[0].id
            course = Course(**course_data, created_at=datetime.utcnow() - timedelta(days=random.randint(10, 180)))
            created_courses.append(course)
        db.add_all(created_courses)
        await db.commit()
        refreshed_courses = []
        for course_obj_stub in created_courses:
            await db.refresh(course_obj_stub)
            refreshed_courses.append(course_obj_stub)

        print("Creating sample materials...")
        all_materials = []
        material_content_examples = {
            "video": "Контент для видео-урока: ссылка на vimeo.com/123456 или подробное описание видео.",
            "text": "Это основной текст для текстового материала. Он может содержать **форматирование**, списки и другую информацию, полезную для изучения темы. Длина этого текста может быть значительной.",
            "quiz": json.dumps({"questions": [{"q": "Что выведет print('Hello, ' + 'World!')?", "options": ["HelloWorld", "Hello, World!", "Ошибка"], "answer": "Hello, World!"}, {"q": "Какой тип данных у значения 3.14?", "options":["int", "float", "str"], "answer":"float"}]}),
            "assignment": "Описание задания: Напишите функцию на Python, которая принимает два числа и возвращает их сумму. Требования к сдаче: файл .py с функцией и тестами."
        }
        for course in refreshed_courses:
            num_materials = random.randint(2, 5)
            for j in range(1, num_materials + 1):
                mat_type = random.choice(["video", "text", "quiz", "assignment"])
                material = Material(course_id=course.id, title=f"Раздел {j}: {course.title.split(':')[0]} - {mat_type.capitalize()}",
                                    content=material_content_examples.get(mat_type, f"Пример контента для типа '{mat_type}'."), type=mat_type,
                                    order_index=j, created_at=course.created_at + timedelta(days=j))
                all_materials.append(material)
        db.add_all(all_materials)
        await db.commit()
        refreshed_materials = []
        for mat_obj_stub in all_materials:
            await db.refresh(mat_obj_stub)
            refreshed_materials.append(mat_obj_stub)
        
        print("Creating sample activities...")
        active_students = [u for u in refreshed_users if u.role == "student" and u.is_active]
        created_activities = []
        if active_students and refreshed_materials:
            for student in active_students:
                num_courses_to_interact = random.randint(1, min(3, len(refreshed_courses)))
                courses_for_student = random.sample(refreshed_courses, num_courses_to_interact)
                for course_obj in courses_for_student:
                    materials_in_this_course = [m for m in refreshed_materials if m.course_id == course_obj.id]
                    if not materials_in_this_course: continue
                    num_materials_to_interact = random.randint(max(1, len(materials_in_this_course) // 2), len(materials_in_this_course))
                    materials_for_interaction = random.sample(materials_in_this_course, num_materials_to_interact)
                    for material in materials_for_interaction:
                        action = random.choice(["view", "start", "complete"])
                        is_completed_action = action == "complete" or (material.type != "quiz" and random.random() < 0.7)
                        activity = Activity(user_id=student.id, material_id=material.id, action="complete" if is_completed_action else action,
                                            timestamp=material.created_at + timedelta(days=random.randint(1, 20), hours=random.randint(0,23)),
                                            duration=random.uniform(300, 3600) if action != "start" else None,
                                            score=round(random.uniform(65, 95), 0) if material.type == "quiz" and is_completed_action else None,
                                            meta={"device": random.choice(["desktop", "mobile"])})
                        created_activities.append(activity)
                        if material.type == "quiz" and is_completed_action and activity.score is not None:
                             created_activities.append(Activity(user_id=student.id, material_id=material.id, action="submit_quiz", 
                                                                timestamp=activity.timestamp + timedelta(seconds=random.randint(5,60)), score=activity.score, meta=activity.meta))
            if created_activities:
                db.add_all(created_activities)
                await db.commit()
        else: print("No active students or materials for activities.")

        user_count = (await db.execute(select(func.count(User.id)))).scalar_one()
        course_count = (await db.execute(select(func.count(Course.id)))).scalar_one()
        material_count = (await db.execute(select(func.count(Material.id)))).scalar_one()
        activity_count = (await db.execute(select(func.count(Activity.id)))).scalar_one()

        print(f"\n--- Database Populated ---")
        print(f"  - Total Users: {user_count}")
        print(f"  - Total Courses: {course_count}")
        print(f"  - Total Materials: {material_count}")
        print(f"  - Total Activities: {activity_count}")

if __name__ == "__main__":
    asyncio.run(fill_with_sample_data())