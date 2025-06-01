# main.py
import os
from pathlib import Path
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
from io import StringIO
import csv
import json

from fastapi import FastAPI, Query, HTTPException, Depends, status, Request
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, StreamingResponse

from jose import JWTError, jwt
from passlib.context import CryptContext
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload
from sqlalchemy import or_, and_, func, desc
from collections import Counter

# --- Локальные импорты ---
from db import SessionLocal, User as DBUser, Course as DBCourse, Material as DBMaterial, Activity as DBActivity
from search_service import (
    init_whoosh_indexes, index_course_item, index_material_item, index_teacher_item,
    delete_item_from_index, search_whoosh, ix_courses, ix_materials, ix_teachers, INDEX_DIR
)

# --- КОНФИГУРАЦИЯ ---
SECRET_KEY = "your-super-secret-key-for-jwt-dont-use-in-prod-change-it" 
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60 * 24 

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

app = FastAPI(
    title="Online Courses Platform API",
    description="Educational platform with course management, search, analytics, and ETL for recommendations.",
    version="1.2.4" # Обновляем версию
)

# --- MIDDLEWARE ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- СТАТИЧЕСКИЕ ФАЙЛЫ И ШАБЛОНЫ ---
BASE_DIR = Path(__file__).resolve().parent
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")
templates = Jinja2Templates(directory=BASE_DIR / "templates")

# --- СОБЫТИЯ FastAPI ---
@app.on_event("startup")
async def startup_event():
    print("FastAPI application startup...")
    try:
        init_whoosh_indexes()
        print("Whoosh indexes are ready.")
    except Exception as e:
        print(f"CRITICAL ERROR during Whoosh initialization on startup: {e}")
        import traceback
        traceback.print_exc()

# --- PYDANTIC МОДЕЛИ (Схемы) ---
class Token(BaseModel):
    access_token: str
    token_type: str

class UserBase(BaseModel):
    name: str
    email: str
    role: str = Field(default="student", pattern="^(student|teacher|admin)$")

class UserCreate(UserBase):
    password: str = Field(min_length=6)

class UserUpdate(BaseModel):
    name: Optional[str] = None
    email: Optional[str] = None
    role: Optional[str] = Field(None, pattern="^(student|teacher|admin)$")
    is_active: Optional[bool] = None

class UserInDB(UserBase):
    id: int
    is_active: bool
    created_at: datetime
    class Config:
        from_attributes = True

class CourseBase(BaseModel):
    title: str = Field(min_length=3, max_length=200)
    description: Optional[str] = None
    category: str = Field(min_length=2, max_length=50)
    level: str = Field(pattern="^(beginner|intermediate|advanced)$")
    teacher_id: Optional[int] = None
    tags: Optional[str] = None 

class CourseCreate(CourseBase):
    pass

class CourseUpdate(BaseModel):
    title: Optional[str] = Field(None, min_length=3, max_length=200)
    description: Optional[str] = None
    category: Optional[str] = Field(None, min_length=2, max_length=50)
    level: Optional[str] = Field(None, pattern="^(beginner|intermediate|advanced)$")
    teacher_id: Optional[int] = None
    tags: Optional[str] = None

class CourseInDB(CourseBase):
    id: int
    created_at: datetime
    class Config:
        from_attributes = True

class MaterialBase(BaseModel):
    title: str = Field(min_length=3, max_length=200)
    content: Optional[str] = None
    type: str = Field(pattern="^(video|text|quiz|assignment)$")
    course_id: int
    order_index: int = 0

class MaterialCreate(MaterialBase):
    pass

class MaterialUpdate(BaseModel):
    title: Optional[str] = Field(None, min_length=3, max_length=200)
    content: Optional[str] = None
    type: Optional[str] = Field(None, pattern="^(video|text|quiz|assignment)$")
    order_index: Optional[int] = None
    course_id: Optional[int] = None

class MaterialInDB(MaterialBase):
    id: int
    created_at: datetime
    class Config:
        from_attributes = True

class ActivityBase(BaseModel):
    material_id: int
    action: str = Field(min_length=3, max_length=50)
    duration: Optional[float] = Field(None, ge=0)
    score: Optional[float] = Field(None, ge=0, le=100)
    meta: Optional[Dict[str, Any]] = None

class ActivityCreate(ActivityBase):
    user_id: int

class ActivityInDB(ActivityBase):
    id: int
    user_id: int
    timestamp: datetime
    class Config:
        from_attributes = True

class SearchFacetValue(BaseModel):
    value: str
    count: int

class SearchFacets(BaseModel):
    categories: List[SearchFacetValue] = []
    levels: List[SearchFacetValue] = []
    tags: List[SearchFacetValue] = []
    material_types: List[SearchFacetValue] = []
    teachers: List[SearchFacetValue] = []

class SearchResultItem(BaseModel):
    id: int
    title: str
    type: str
    description: Optional[str] = None
    category: Optional[str] = None
    level: Optional[str] = None
    teacher_name: Optional[str] = None
    material_type_field: Optional[str] = None
    tags: List[str] = []
    relevance_score: Optional[float] = None

class SearchResponse(BaseModel):
    query: Optional[str]
    filters: Dict[str, Optional[Any]]
    results: List[SearchResultItem]
    facets: SearchFacets

class UserCourseInteractionETL(BaseModel):
    user_id: int
    course_id: int
    user_name: Optional[str] = None
    course_title: Optional[str] = None
    completed_materials_count: int = 0
    total_materials_in_course: int = 0
    progress_percentage: float = 0.0
    total_time_spent_seconds: float = 0.0
    actions_count: int = 0
    avg_score_on_quizzes: Optional[float] = None
    last_activity_timestamp: Optional[datetime] = None
    first_activity_timestamp: Optional[datetime] = None

class CourseFeatureETL(BaseModel):
    course_id: int
    title: str
    description: Optional[str] = None
    category: str
    level: str
    teacher_id: Optional[int] = None
    teacher_name: Optional[str] = None
    created_at: datetime
    num_materials: int = 0
    tags: List[str] = []

class UserFeatureETL(BaseModel):
    user_id: int
    user_name: str
    user_email: str
    role: str
    created_at: datetime
    is_active: bool
    total_courses_interacted_with: int = 0
    total_courses_completed: int = 0
    total_activities_logged: int = 0
    total_time_spent_learning_seconds: float = 0.0
    avg_progress_on_interacted_courses: Optional[float] = None
    avg_score_on_all_quizzes: Optional[float] = None
    preferred_categories: List[str] = []
    preferred_levels: List[str] = []

# --- Dependencies & Auth Helpers ---
async def get_db():
    async with SessionLocal() as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()

def verify_password(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)

def get_password_hash(password: str) -> str:
    return pwd_context.hash(password)

def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    to_encode = data.copy()
    expire_time = datetime.utcnow() + (expires_delta or timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES))
    to_encode.update({"exp": expire_time, "sub": str(data.get("sub"))})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

async def get_current_user(token: str = Depends(oauth2_scheme), db: AsyncSession = Depends(get_db)) -> DBUser:
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials", headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        user_id_str: Optional[str] = payload.get("sub")
        if user_id_str is None: raise credentials_exception
        user_id = int(user_id_str)
    except (JWTError, ValueError):
        raise credentials_exception
    
    user = await db.get(DBUser, user_id)
    if user is None or not user.is_active:
        raise credentials_exception
    return user

def require_role(*roles: str):
    async def role_checker(current_user: DBUser = Depends(get_current_user)):
        if current_user.role not in roles:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=f"Operation not permitted for role: {current_user.role}")
        return current_user
    return role_checker

# --- Frontend Routes ---
@app.get("/", response_class=HTMLResponse, tags=["Frontend"])
async def read_index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request, "year": datetime.utcnow().year})

@app.get("/dashboard", response_class=HTMLResponse, tags=["Frontend"])
async def read_dashboard(request: Request, current_user: UserInDB = Depends(get_current_user)):
    return templates.TemplateResponse("dashboard.html", {"request": request, "user": current_user, "year": datetime.utcnow().year})

# --- API Endpoints ---

# Auth
@app.post("/token", response_model=Token, tags=["Auth"])
async def login_for_access_token_api(form_data: OAuth2PasswordRequestForm = Depends(), db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(DBUser).where(DBUser.email == form_data.username))
    user = result.scalar_one_or_none()
    if not user or not user.is_active or not verify_password(form_data.password, user.password_hash):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Incorrect email or password, or user inactive")
    
    token_data = {"sub": user.id, "role": user.role}
    access_token = create_access_token(data=token_data)
    return {"access_token": access_token, "token_type": "bearer"}

# Users
@app.post("/users/register", response_model=UserInDB, status_code=status.HTTP_201_CREATED, tags=["Users"])
async def register_new_user_api(user_in: UserCreate, db: AsyncSession = Depends(get_db)):
    existing_user_res = await db.execute(select(DBUser).where(DBUser.email == user_in.email))
    if existing_user_res.scalar_one_or_none():
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Email already registered")
    
    hashed_password = get_password_hash(user_in.password)
    db_user_obj = DBUser(
        name=user_in.name, email=user_in.email, role=user_in.role,
        password_hash=hashed_password, created_at=datetime.utcnow(), is_active=True
    )
    db.add(db_user_obj)
    await db.commit() 
    await db.refresh(db_user_obj)

    if db_user_obj.role == "teacher":
        index_teacher_item(db_id=db_user_obj.id, name=db_user_obj.name)
    return db_user_obj

@app.get("/users/me", response_model=UserInDB, tags=["Users"])
async def read_current_user_me_api(current_user: DBUser = Depends(get_current_user)):
    return current_user

# Courses
@app.post("/courses", response_model=CourseInDB, status_code=status.HTTP_201_CREATED, tags=["Courses"])
async def create_course_api(course_in: CourseCreate, db: AsyncSession = Depends(get_db), current_user: DBUser = Depends(require_role("admin", "teacher"))):
    teacher_name_for_index = ""
    final_teacher_id = course_in.teacher_id

    if current_user.role == "teacher":
        final_teacher_id = current_user.id
        teacher_name_for_index = current_user.name
    elif final_teacher_id is not None: # Админ назначает преподавателя
        teacher_res = await db.execute(select(DBUser).where(DBUser.id == final_teacher_id, DBUser.role == "teacher"))
        teacher = teacher_res.scalar_one_or_none()
        if not teacher:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Assigned teacher with id {final_teacher_id} not found or not a teacher.")
        teacher_name_for_index = teacher.name
    # Если админ и teacher_id не указан (None), он остается None, teacher_name_for_index будет ""
    
    course_dict = course_in.dict()
    course_dict["teacher_id"] = final_teacher_id
    
    db_course_obj = DBCourse(**course_dict, created_at=datetime.utcnow())
    db.add(db_course_obj)
    await db.commit()
    await db.refresh(db_course_obj)
    index_course_item(
        db_id=db_course_obj.id, title=db_course_obj.title, category=db_course_obj.category,
        level=db_course_obj.level, teacher_name=teacher_name_for_index,
        description=db_course_obj.description, tags=db_course_obj.tags
    )
    return db_course_obj

@app.get("/courses", response_model=List[CourseInDB], tags=["Courses"])
async def get_all_courses_api(skip: int = 0, limit: int = 20, category: Optional[str] = None, level: Optional[str] = None, teacher_id: Optional[int] = None, db: AsyncSession = Depends(get_db)):
    stmt = select(DBCourse).order_by(desc(DBCourse.created_at))
    if category: stmt = stmt.where(DBCourse.category.ilike(f"%{category}%"))
    if level: stmt = stmt.where(DBCourse.level == level)
    if teacher_id: stmt = stmt.where(DBCourse.teacher_id == teacher_id)
    stmt = stmt.offset(skip).limit(limit)
    result = await db.execute(stmt)
    return result.scalars().all()

@app.get("/courses/{course_id}", response_model=CourseInDB, tags=["Courses"])
async def get_course_api(course_id: int, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(DBCourse).options(selectinload(DBCourse.teacher)).where(DBCourse.id == course_id))
    course = result.scalar_one_or_none()
    if not course: raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Course not found")
    return course

@app.patch("/courses/{course_id}", response_model=CourseInDB, tags=["Courses"])
async def update_course_api(course_id: int, course_update: CourseUpdate, db: AsyncSession = Depends(get_db), current_user: DBUser = Depends(require_role("admin", "teacher"))):
    result = await db.execute(select(DBCourse).options(selectinload(DBCourse.teacher)).where(DBCourse.id == course_id))
    db_course_obj = result.scalar_one_or_none()
    if not db_course_obj: raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Course not found")
    if current_user.role == "teacher" and db_course_obj.teacher_id != current_user.id:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Not authorized to update this course")
    
    update_data = course_update.dict(exclude_unset=True)
    final_teacher_name = db_course_obj.teacher.name if db_course_obj.teacher else ""

    for key, value in update_data.items():
        setattr(db_course_obj, key, value)
        if key == "teacher_id":
            if value is not None:
                if current_user.role == "teacher" and value != current_user.id:
                     raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Teachers can only assign courses to themselves or remove assignment.")
                teacher_res = await db.execute(select(DBUser.name).where(DBUser.id == value, DBUser.role == "teacher"))
                new_name = teacher_res.scalar_one_or_none()
                if not new_name: raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"New teacher with id {value} not found or not a teacher.")
                final_teacher_name = new_name
            else:
                final_teacher_name = ""
            
    await db.commit()
    await db.refresh(db_course_obj)
    
    if "teacher_id" in update_data:
        if db_course_obj.teacher_id:
            teacher_obj_refreshed = await db.get(DBUser, db_course_obj.teacher_id)
            final_teacher_name = teacher_obj_refreshed.name if teacher_obj_refreshed and teacher_obj_refreshed.role == 'teacher' else ""
        else:
            final_teacher_name = ""
            
    index_course_item(
        db_id=db_course_obj.id, title=db_course_obj.title, category=db_course_obj.category,
        level=db_course_obj.level, teacher_name=final_teacher_name,
        description=db_course_obj.description, tags=db_course_obj.tags
    )
    return db_course_obj

@app.delete("/courses/{course_id}", status_code=status.HTTP_204_NO_CONTENT, tags=["Courses"])
async def delete_course_api(course_id: int, db: AsyncSession = Depends(get_db), current_user: DBUser = Depends(require_role("admin", "teacher"))):
    result = await db.execute(select(DBCourse).where(DBCourse.id == course_id))
    db_course_obj = result.scalar_one_or_none()
    if not db_course_obj: raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Course not found")
    if current_user.role == "teacher" and db_course_obj.teacher_id != current_user.id:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Not authorized to delete this course")
    
    delete_item_from_index(ix_courses, db_id=course_id)
    materials_of_course_res = await db.execute(select(DBMaterial.id).where(DBMaterial.course_id == course_id))
    for mat_id_tuple in materials_of_course_res.all():
        delete_item_from_index(ix_materials, db_id=mat_id_tuple[0])
    
    await db.delete(db_course_obj)
    await db.commit()

# Materials
@app.post("/materials", response_model=MaterialInDB, status_code=status.HTTP_201_CREATED, tags=["Materials"])
async def create_material_api(material_in: MaterialCreate, db: AsyncSession = Depends(get_db), current_user: DBUser = Depends(require_role("admin", "teacher"))):
    course_res = await db.execute(select(DBCourse).where(DBCourse.id == material_in.course_id))
    course_obj = course_res.scalar_one_or_none()
    if not course_obj: raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Course with id {material_in.course_id} not found.")
    if current_user.role == "teacher" and course_obj.teacher_id != current_user.id:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Not authorized to add material to this course")
    
    db_material_obj = DBMaterial(**material_in.dict(exclude_unset=True), created_at=datetime.utcnow())
    db.add(db_material_obj)
    await db.commit()
    await db.refresh(db_material_obj)
    index_material_item(
        db_id=db_material_obj.id, title=db_material_obj.title, material_type=db_material_obj.type,
        course_id_ref=db_material_obj.course_id, course_title_ref=course_obj.title, content=db_material_obj.content
    )
    return db_material_obj

@app.get("/courses/{course_id}/materials", response_model=List[MaterialInDB], tags=["Materials"])
async def get_course_materials_api(course_id: int, db: AsyncSession = Depends(get_db)):
    course_exists_res = await db.execute(select(DBCourse.id).where(DBCourse.id == course_id))
    if not course_exists_res.scalar_one_or_none():
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Course not found")
    result = await db.execute(select(DBMaterial).where(DBMaterial.course_id == course_id).order_by(DBMaterial.order_index))
    return result.scalars().all()

# Search
@app.get("/search", response_model=SearchResponse, tags=["Search"])
async def search_content_api(
    query: Optional[str] = Query(None, alias="q"), 
    category: Optional[str] = Query(None), 
    level: Optional[str] = Query(None),
    tags: Optional[List[str]] = Query(None), 
    material_type: Optional[str] = Query(None), 
    teacher_name: Optional[str] = Query(None),
    search_in_courses: bool = Query(True), 
    search_in_materials: bool = Query(True), 
    search_in_teachers: bool = Query(True),
    limit: int = Query(20, ge=1, le=100)
):
    if not any([query, category, level, tags, material_type, teacher_name]):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Please provide a search query or at least one filter.")
    
    effective_tags = None
    if tags:
        parsed_tags = []
        for tag_group in tags:
            if isinstance(tag_group, str):
                parsed_tags.extend([t.strip().lower() for t in tag_group.split(',') if t.strip()])
        if parsed_tags:
            effective_tags = list(set(parsed_tags))

    results_from_whoosh, facets_data_raw = search_whoosh(
        query_str=query, search_in_courses=search_in_courses, search_in_materials=search_in_materials, 
        search_in_teachers=search_in_teachers, filter_category=category, filter_level=level, 
        filter_tags=effective_tags, filter_material_type=material_type, 
        filter_teacher_name=teacher_name, limit=limit
    )
    
    applied_filters: Dict[str, Optional[Any]] = {
        "category": category, "level": level, "tags": effective_tags, 
        "material_type": material_type, "teacher_name": teacher_name
    }
    pydantic_results = [SearchResultItem(**item) for item in results_from_whoosh]
    
    processed_facets = SearchFacets(
        categories=[SearchFacetValue(value=str(k), count=v) for k, v in facets_data_raw.get("categories", {}).items()],
        levels=[SearchFacetValue(value=str(k), count=v) for k, v in facets_data_raw.get("levels", {}).items()],
        tags=[SearchFacetValue(value=str(k), count=v) for k, v in facets_data_raw.get("tags", {}).items()],
        material_types=[SearchFacetValue(value=str(k), count=v) for k, v in facets_data_raw.get("material_types", {}).items()],
        teachers=[SearchFacetValue(value=str(k), count=v) for k, v in facets_data_raw.get("teachers", {}).items()]
    )
    return SearchResponse(query=query, filters=applied_filters, results=pydantic_results, facets=processed_facets)

# Admin Search Utils
async def _perform_full_reindex(db: AsyncSession):
    print("Starting full reindex of all data for Whoosh...")
    if INDEX_DIR.exists():
        import shutil
        for sub_index_name in ["courses", "materials", "teachers"]:
            sub_index_path = INDEX_DIR / sub_index_name
            if sub_index_path.exists() and sub_index_path.is_dir():
                shutil.rmtree(sub_index_path)
        print("Whoosh index subdirectories cleared.")
    
    init_whoosh_indexes() 

    courses_res = await db.execute(select(DBCourse).options(selectinload(DBCourse.teacher)))
    course_count = 0
    for course in courses_res.scalars().all():
        teacher_name_val = course.teacher.name if course.teacher else ""
        index_course_item(course.id, course.title, course.category, course.level, teacher_name_val, course.description, course.tags)
        course_count+=1
    print(f"Reindexed {course_count} courses.")

    materials_res = await db.execute(select(DBMaterial).options(selectinload(DBMaterial.course)))
    material_count = 0
    for material in materials_res.scalars().all():
        course_title_val = material.course.title if material.course else "N/A"
        index_material_item(material.id, material.title, material.type, material.course_id, course_title_val, material.content)
        material_count +=1
    print(f"Reindexed {material_count} materials.")

    teachers_res = await db.execute(select(DBUser).where(DBUser.role == 'teacher'))
    teacher_count = 0
    for teacher in teachers_res.scalars().all():
        index_teacher_item(teacher.id, teacher.name)
        teacher_count+=1
    print(f"Reindexed {teacher_count} teachers.")
    print("Full reindex completed successfully.")

@app.post("/admin/search/reindex-all", status_code=status.HTTP_202_ACCEPTED, tags=["Admin Search Utils"])
async def trigger_full_reindex_api(db: AsyncSession = Depends(get_db), current_user: DBUser = Depends(require_role("admin"))):
    try:
        await _perform_full_reindex(db)
        return {"message": "Full reindexing process completed (synchronously)."}
    except Exception as e:
        print(f"Error during reindexing: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Reindexing failed: {str(e)}")

# Activity Logging
@app.post("/activities", response_model=ActivityInDB, tags=["Activity"])
async def create_activity_api(activity_in: ActivityCreate, db: AsyncSession = Depends(get_db), current_user: DBUser = Depends(get_current_user)):
    if activity_in.user_id != current_user.id and current_user.role != "admin":
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Not authorized to log activity for another user.")
    
    material_exists = await db.execute(select(DBMaterial.id).where(DBMaterial.id == activity_in.material_id))
    if not material_exists.scalar_one_or_none():
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Material with id {activity_in.material_id} not found.")
    
    db_activity = DBActivity(**activity_in.dict(), timestamp=datetime.utcnow())
    db.add(db_activity)
    await db.commit()
    await db.refresh(db_activity)
    return db_activity

# Analytics
@app.get("/analytics/user/{user_id}/progress", response_model=List[Dict[str, Any]], tags=["Analytics"])
async def get_user_progress_api(user_id: int, db: AsyncSession = Depends(get_db), current_user: DBUser = Depends(get_current_user)):
    if user_id != current_user.id and current_user.role not in ["admin", "teacher"]:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Not authorized to view this user's progress.")
    
    stmt = (
        select(DBActivity, DBMaterial, DBCourse)
        .join(DBMaterial, DBActivity.material_id == DBMaterial.id)
        .join(DBCourse, DBMaterial.course_id == DBCourse.id)
        .where(DBActivity.user_id == user_id)
    )
    result = await db.execute(stmt)
    activities_data = result.all()
    
    course_progress_map: Dict[int, Dict[str, Any]] = {}
    for activity, material, course_obj in activities_data:
        key = course_obj.id
        if key not in course_progress_map:
            # Получаем общее количество материалов в курсе один раз
            materials_count_stmt = select(func.count(DBMaterial.id)).where(DBMaterial.course_id == course_obj.id)
            total_materials = (await db.execute(materials_count_stmt)).scalar_one_or_none() or 0
            course_progress_map[key] = {
                "course_id": course_obj.id, "course_title": course_obj.title,
                "total_materials": total_materials, "completed_materials": 0, # Будем считать уникальные материалы
                "completed_material_ids": set(), # Для подсчета уникальных
                "total_time": 0.0, "scores": []
            }
        
        progress_item = course_progress_map[key]
        if activity.action == "complete":
            progress_item["completed_material_ids"].add(material.id)
        
        if activity.duration: progress_item["total_time"] += activity.duration
        if activity.score is not None: progress_item["scores"].append(activity.score)
            
    response_list = []
    for data_item in course_progress_map.values():
        data_item["completed_materials"] = len(data_item["completed_material_ids"]) # Уникальные завершенные
        data_item["completion_percentage"] = (data_item["completed_materials"] / data_item["total_materials"] * 100) if data_item["total_materials"] > 0 else 0.0
        data_item["avg_score"] = round(sum(data_item["scores"]) / len(data_item["scores"]), 2) if data_item["scores"] else None
        del data_item["scores"]
        del data_item["completed_material_ids"] # Удаляем вспомогательное поле
        response_list.append(data_item)
        
    return response_list

# ETL Endpoints
@app.get("/etl/user_course_interactions", response_model=List[UserCourseInteractionETL], tags=["ETL & Data Preparation"])
async def etl_get_user_course_interactions_api(db: AsyncSession = Depends(get_db), current_user_admin: DBUser = Depends(require_role("admin"))):
    print("ETL: Starting User-Course Interactions data preparation...")
    course_material_counts: Dict[int, int] = {}
    all_materials_in_courses_stmt = select(DBCourse.id, func.count(DBMaterial.id).label("material_count")).outerjoin(DBMaterial, DBCourse.id == DBMaterial.course_id).group_by(DBCourse.id)
    all_materials_res = await db.execute(all_materials_in_courses_stmt)
    for course_id_val, count_val in all_materials_res.all(): course_material_counts[course_id_val] = count_val

    stmt = (select(DBActivity.user_id, DBMaterial.course_id, DBUser.name.label("user_name"), DBCourse.title.label("course_title"), DBActivity.action, DBActivity.timestamp, DBActivity.duration, DBActivity.score, DBMaterial.type.label("material_type"), DBMaterial.id.label("material_id"))
        .join(DBUser, DBActivity.user_id == DBUser.id).join(DBMaterial, DBActivity.material_id == DBMaterial.id).join(DBCourse, DBMaterial.course_id == DBCourse.id)
        .order_by(DBActivity.user_id, DBMaterial.course_id, DBActivity.timestamp))
    all_activities_result = await db.execute(stmt)
    
    user_course_data: Dict[tuple[int, int], Dict[str, Any]] = {}
    raw_activity_count = 0
    cleaned_activity_count = 0

    for row in all_activities_result.mappings().all():
        raw_activity_count += 1
        if row.action == "view" and (row.duration is None or row.duration <= 10): # Увеличил порог для view
            continue 
        current_score = row.score
        if row.score is not None and not (0 <= row.score <= 100):
            current_score = None
        cleaned_activity_count +=1
        key = (row.user_id, row.course_id)
        if key not in user_course_data: 
            user_course_data[key] = {"user_id": row.user_id, "course_id": row.course_id, "user_name": row.user_name, "course_title": row.course_title, 
                                   "completed_material_ids": set(), "total_time_spent_seconds": 0.0, "actions_count": 0, 
                                   "quiz_scores": [], "first_activity_timestamp": row.timestamp, "last_activity_timestamp": row.timestamp}
        agg_data = user_course_data[key]; agg_data["actions_count"] += 1
        if row.duration and row.duration > 0 : agg_data["total_time_spent_seconds"] += row.duration
        if row.action == "complete": agg_data["completed_material_ids"].add(row.material_id)
        if row.material_type == "quiz" and current_score is not None: agg_data["quiz_scores"].append(current_score)
        if row.timestamp > agg_data["last_activity_timestamp"]: agg_data["last_activity_timestamp"] = row.timestamp
    
    print(f"ETL [User-Course]: Processed {raw_activity_count} raw activities, {cleaned_activity_count} after cleaning.")
    etl_results: List[UserCourseInteractionETL] = []
    for agg_data in user_course_data.values():
        total_materials = course_material_counts.get(agg_data["course_id"], 0)
        completed_count = len(agg_data["completed_material_ids"])
        progress = (completed_count / total_materials * 100) if total_materials > 0 else 0.0
        avg_score = sum(agg_data["quiz_scores"]) / len(agg_data["quiz_scores"]) if agg_data["quiz_scores"] else None
        etl_results.append(UserCourseInteractionETL(user_id=agg_data["user_id"], course_id=agg_data["course_id"], user_name=agg_data["user_name"], course_title=agg_data["course_title"], completed_materials_count=completed_count, total_materials_in_course=total_materials, progress_percentage=round(progress, 2), total_time_spent_seconds=round(agg_data["total_time_spent_seconds"], 2), actions_count=agg_data["actions_count"], avg_score_on_quizzes=round(avg_score, 2) if avg_score is not None else None, first_activity_timestamp=agg_data["first_activity_timestamp"], last_activity_timestamp=agg_data["last_activity_timestamp"]))
    print(f"ETL: User-Course Interactions data preparation complete. Generated {len(etl_results)} records.")
    return etl_results

@app.get("/etl/course_features", response_model=List[CourseFeatureETL], tags=["ETL & Data Preparation"])
async def etl_get_course_features_api(db: AsyncSession = Depends(get_db), current_user_admin: DBUser = Depends(require_role("admin"))):
    print("ETL: Starting Course Features data preparation...")
    stmt = (select(DBCourse, DBUser.name.label("teacher_name_alias"), func.count(DBMaterial.id).label("num_materials_alias"))
        .outerjoin(DBUser, DBCourse.teacher_id == DBUser.id).outerjoin(DBMaterial, DBCourse.id == DBMaterial.course_id)
        .group_by(DBCourse.id, DBUser.name).order_by(DBCourse.id))
    result = await db.execute(stmt)
    course_features_list: List[CourseFeatureETL] = []
    raw_course_count = 0; cleaned_course_count = 0
    for course_row, teacher_name_val, num_materials_val in result.all():
        raw_course_count += 1
        cleaned_description = course_row.description
        if course_row.description: cleaned_description = " ".join(course_row.description.split()) 
        tag_list = [t.strip().lower() for t in course_row.tags.split(',')] if course_row.tags and course_row.tags.strip() else []
        tag_list = [t for t in tag_list if t]
        if len(course_row.title) < 3: continue
        cleaned_course_count +=1
        course_features_list.append(CourseFeatureETL(course_id=course_row.id, title=course_row.title, description=cleaned_description, category=course_row.category, level=course_row.level, teacher_id=course_row.teacher_id, teacher_name=teacher_name_val, created_at=course_row.created_at, num_materials=num_materials_val or 0, tags=tag_list))
    print(f"ETL [CourseFeatures]: Processed {raw_course_count} raw courses, {cleaned_course_count} after cleaning.")
    print(f"ETL: Course Features data preparation complete. Generated {len(course_features_list)} records.")
    return course_features_list

@app.get("/etl/user_features", response_model=List[UserFeatureETL], tags=["ETL & Data Preparation"])
async def etl_get_user_features_api(db: AsyncSession = Depends(get_db), current_user_admin: DBUser = Depends(require_role("admin"))):
    print("ETL: Starting User Features data preparation...")
    user_course_interactions_list: List[UserCourseInteractionETL] = await etl_get_user_course_interactions_api(db=db, current_user_admin=current_user_admin)
    user_interactions_map: Dict[int, List[UserCourseInteractionETL]] = {}
    for interaction in user_course_interactions_list: user_interactions_map.setdefault(interaction.user_id, []).append(interaction)
    all_courses_info_stmt = select(DBCourse.id, DBCourse.category, DBCourse.level); all_courses_info_res = await db.execute(all_courses_info_stmt)
    courses_meta_map = {row.id: {"category": row.category, "level": row.level} for row in all_courses_info_res.mappings().all()}
    all_users_result = await db.execute(select(DBUser)); users_from_db = all_users_result.scalars().all()
    user_features_list: List[UserFeatureETL] = []
    raw_user_count = 0; cleaned_user_count = 0
    for db_user in users_from_db:
        raw_user_count +=1
        if not db_user.is_active: continue
        if "@" not in db_user.email or "." not in db_user.email.split("@")[-1]: continue
        cleaned_user_count +=1
        interactions = user_interactions_map.get(db_user.id, [])
        total_courses_interacted = len(interactions); total_courses_completed = sum(1 for i in interactions if i.progress_percentage >= 100.0)
        total_time_spent_learning = sum(i.total_time_spent_seconds for i in interactions)
        avg_progress = sum(i.progress_percentage for i in interactions) / len(interactions) if interactions else None
        user_activities_stmt = select(DBActivity.action, DBActivity.score, DBMaterial.type).join(DBMaterial).where(DBActivity.user_id == db_user.id); user_activities_res = await db.execute(user_activities_stmt)
        total_activities_logged = 0; all_quiz_scores = []
        for act_row in user_activities_res.mappings().all():
            total_activities_logged += 1
            if act_row.type == "quiz" and act_row.score is not None and (0 <= act_row.score <= 100): all_quiz_scores.append(act_row.score)
        avg_score_all_quizzes = sum(all_quiz_scores) / len(all_quiz_scores) if all_quiz_scores else None
        category_counter = Counter(); level_counter = Counter()
        for interaction in interactions:
            if interaction.course_id and interaction.progress_percentage > 30:
                course_meta = courses_meta_map.get(interaction.course_id)
                if course_meta: category_counter[course_meta["category"]] +=1; level_counter[course_meta["level"]] +=1
        user_features_list.append(UserFeatureETL(user_id=db_user.id, user_name=db_user.name, user_email=db_user.email, role=db_user.role, created_at=db_user.created_at, is_active=db_user.is_active, total_courses_interacted_with=total_courses_interacted, total_courses_completed=total_courses_completed, total_activities_logged=total_activities_logged, total_time_spent_learning_seconds=round(total_time_spent_learning, 2), avg_progress_on_interacted_courses=round(avg_progress, 2) if avg_progress is not None else None, avg_score_on_all_quizzes=round(avg_score_all_quizzes, 2) if avg_score_all_quizzes is not None else None, preferred_categories=[c for c, _ in category_counter.most_common(3)], preferred_levels=[l for l, _ in level_counter.most_common(3)]))
    print(f"ETL [UserFeatures]: Processed {raw_user_count} raw users, {cleaned_user_count} after cleaning.")
    print(f"ETL: User Features data preparation complete. Generated {len(user_features_list)} records.")
    return user_features_list

@app.get("/etl/export/user_course_interactions_csv", response_class=StreamingResponse, tags=["ETL Export"])
async def export_user_course_interactions_csv_api(db: AsyncSession = Depends(get_db), current_user_admin: DBUser = Depends(require_role("admin"))):
    data_list = await etl_get_user_course_interactions_api(db=db, current_user_admin=current_user_admin)
    if not data_list: return StreamingResponse(iter(["No data to export."]), media_type="text/plain")
    output = StringIO(); fieldnames = list(UserCourseInteractionETL.model_fields.keys())
    writer = csv.DictWriter(output, fieldnames=fieldnames); writer.writeheader()
    for item_model in data_list: writer.writerow(item_model.model_dump(mode='json'))
    output.seek(0); return StreamingResponse(iter([output.getvalue()]), media_type="text/csv", headers={"Content-Disposition": "attachment; filename=user_course_interactions.csv"})

@app.get("/etl/export/course_features_json", response_model=List[CourseFeatureETL], tags=["ETL Export"])
async def export_course_features_json_api(db: AsyncSession = Depends(get_db), current_user_admin: DBUser = Depends(require_role("admin"))):
    return await etl_get_course_features_api(db=db, current_user_admin=current_user_admin)

@app.get("/etl/export/user_features_json", response_model=List[UserFeatureETL], tags=["ETL Export"])
async def export_user_features_json_api(db: AsyncSession = Depends(get_db), current_user_admin: DBUser = Depends(require_role("admin"))):
    return await etl_get_user_features_api(db=db, current_user_admin=current_user_admin)