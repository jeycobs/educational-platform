# main.py
import os
import re
from pathlib import Path
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
from io import StringIO
import csv
import json
import time  # <--- ДОБАВЛЕНО

from fastapi import FastAPI, Query, HTTPException, Depends, status, Request
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, StreamingResponse

from jose import JWTError, jwt
from passlib.context import CryptContext
from pydantic import BaseModel, Field, EmailStr
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload
from sqlalchemy import or_, and_, func, desc
from collections import Counter

from db import SessionLocal, User as DBUser, Course as DBCourse, Material as DBMaterial, Activity as DBActivity
from search_service import (
    init_whoosh_indexes, index_course_item, index_material_item, index_teacher_item,
    delete_item_from_index, search_whoosh
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
    version="1.5.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE_DIR = Path(__file__).resolve().parent
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")
templates = Jinja2Templates(directory=BASE_DIR / "templates")

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
class Token(BaseModel):
    access_token: str
    token_type: str

class UserBase(BaseModel):
    name: str
    email: EmailStr
    role: str = Field(default="student", pattern="^(student|teacher|admin)$")

class UserCreate(UserBase):
    password: str = Field(min_length=6)

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

class ActivityInDB(ActivityBase):
    id: int
    user_id: int
    timestamp: datetime

    class Config:
        from_attributes = True

class ActivityInDBWithMaterial(ActivityInDB):
    material_title: str
    material_type: str

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
    material_type_field: Optional[str] = Field(None, alias="material_type")
    tags: List[str] = []
    relevance_score: Optional[float] = None

class SearchResponse(BaseModel):
    query: Optional[str]
    filters: Dict[str, Any]
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

async def get_db():
    async with SessionLocal() as session:
        yield session

def verify_password(plain_password: str, hashed_password: str) -> bool: return pwd_context.verify(plain_password, hashed_password)
def get_password_hash(password: str) -> str: return pwd_context.hash(password)
def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    to_encode = data.copy()
    expire_time = datetime.utcnow() + (expires_delta or timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES))
    to_encode.update({"exp": expire_time, "sub": str(data.get("sub"))})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

async def get_current_user(token: str = Depends(oauth2_scheme), db: AsyncSession = Depends(get_db)) -> DBUser:
    credentials_exception = HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Could not validate credentials", headers={"WWW-Authenticate": "Bearer"})
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        user_id_str: Optional[str] = payload.get("sub")
        if user_id_str is None: raise credentials_exception
        user_id = int(user_id_str)
    except (JWTError, ValueError): raise credentials_exception
    user = await db.get(DBUser, user_id)
    if user is None or not user.is_active: raise credentials_exception
    return user

def require_role(*roles: str):
    async def role_checker(current_user: DBUser = Depends(get_current_user)):
        if current_user.role not in roles: raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=f"Operation not permitted for role: '{current_user.role}'")
        return current_user
    return role_checker

# --- Frontend Routes ---
@app.get("/", response_class=HTMLResponse, tags=["Frontend"])
async def read_index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request, "year": datetime.utcnow().year, "now_timestamp": int(time.time())})

@app.get("/dashboard", response_class=HTMLResponse, tags=["Frontend"])
async def read_dashboard(request: Request):
    return templates.TemplateResponse("dashboard.html", {"request": request, "year": datetime.utcnow().year, "now_timestamp": int(time.time())})

@app.get("/courses/{course_id_page}", response_class=HTMLResponse, name="course_detail_page")
async def get_course_detail_page(request: Request, course_id_page: int, db: AsyncSession = Depends(get_db)):
    try:
        result = await db.execute(select(DBCourse).options(selectinload(DBCourse.teacher)).where(DBCourse.id == course_id_page))
        course = result.scalar_one_or_none()
        if not course: raise HTTPException(status_code=404, detail="Course not found")
        teacher_name = course.teacher.name if course.teacher else "Не назначен"
        return templates.TemplateResponse("course_detail.html", {"request": request, "course": course, "teacher_name": teacher_name, "year": datetime.utcnow().year, "now_timestamp": int(time.time())})
    except Exception:
        return templates.TemplateResponse("404.html", {"request": request, "now_timestamp": int(time.time())}, status_code=404)

# --- API Endpoints ---
@app.post("/token", response_model=Token, tags=["Auth"])
async def login_for_access_token_api(form_data: OAuth2PasswordRequestForm = Depends(), db: AsyncSession = Depends(get_db)):
    user_res = await db.execute(select(DBUser).where(DBUser.email == form_data.username))
    user = user_res.scalar_one_or_none()
    if not user or not user.is_active or not verify_password(form_data.password, user.password_hash): raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Неверный email или пароль")
    token_data = {"sub": user.id, "role": user.role}
    return {"access_token": create_access_token(data=token_data), "token_type": "bearer"}

@app.post("/users/register", response_model=UserInDB, status_code=status.HTTP_201_CREATED, tags=["Users"])
async def register_new_user_api(user_in: UserCreate, db: AsyncSession = Depends(get_db)):
    if (await db.execute(select(DBUser.id).where(DBUser.email == user_in.email))).scalar_one_or_none(): raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Email уже зарегистрирован")
    db_user = DBUser(name=user_in.name, email=user_in.email, role=user_in.role, password_hash=get_password_hash(user_in.password))
    db.add(db_user); await db.commit(); await db.refresh(db_user)
    if db_user.role == "teacher": index_teacher_item(db_id=db_user.id, name=db_user.name)
    return db_user

@app.get("/users/me", response_model=UserInDB, tags=["Users"])
async def read_current_user_me_api(current_user: DBUser = Depends(get_current_user)): return current_user

@app.get("/users/me/activities", response_model=List[ActivityInDBWithMaterial], tags=["Users"])
async def get_my_activities_api(limit: int = Query(20, ge=1, le=100), db: AsyncSession = Depends(get_db), current_user: DBUser = Depends(get_current_user)):
    stmt = select(DBActivity, DBMaterial.title, DBMaterial.type).join(DBMaterial).where(DBActivity.user_id == current_user.id).order_by(desc(DBActivity.timestamp)).limit(limit)
    result = await db.execute(stmt)
    return [ActivityInDBWithMaterial(**ActivityInDB.from_orm(act).model_dump(), material_title=m_title, material_type=m_type) for act, m_title, m_type in result.all()]

@app.get("/courses", response_model=List[CourseInDB], tags=["Courses"])
async def get_all_courses_api(skip: int = 0, limit: int = 20, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(DBCourse).order_by(desc(DBCourse.created_at)).offset(skip).limit(limit))
    return result.scalars().all()

@app.post("/courses", response_model=CourseInDB, status_code=status.HTTP_201_CREATED, tags=["Courses"])
async def create_new_course_api(course_in: CourseCreate, db: AsyncSession = Depends(get_db), current_user: DBUser = Depends(require_role("teacher", "admin"))):
    if course_in.teacher_id is None: course_in.teacher_id = current_user.id
    teacher_res = await db.get(DBUser, course_in.teacher_id)
    if not teacher_res or teacher_res.role not in ["teacher", "admin"]: raise HTTPException(status_code=400, detail=f"User with id {course_in.teacher_id} is not a valid teacher.")
    db_course = DBCourse(**course_in.model_dump()); db.add(db_course); await db.commit(); await db.refresh(db_course)
    index_course_item(db_id=db_course.id, title=db_course.title, description=db_course.description, category=db_course.category, level=db_course.level, teacher_name=teacher_res.name, tags=db_course.tags)
    return db_course

@app.get("/courses/{course_id}/materials", response_model=List[MaterialInDB], tags=["Materials"])
async def get_course_materials_api(course_id: int, db: AsyncSession = Depends(get_db)):
    if not (await db.execute(select(DBCourse.id).where(DBCourse.id == course_id))).scalar_one_or_none(): raise HTTPException(status_code=404, detail="Course not found")
    result = await db.execute(select(DBMaterial).where(DBMaterial.course_id == course_id).order_by(DBMaterial.order_index))
    return result.scalars().all()

# --- Search Endpoints ---
@app.post("/admin/search/reindex-all", status_code=status.HTTP_200_OK, tags=["Admin", "Search"])
async def reindex_all_content_api(
    db: AsyncSession = Depends(get_db), 
    current_user: DBUser = Depends(require_role("admin"))
):
    """
    Полностью очищает и заново строит поисковый индекс из данных в БД.
    Доступно только для администраторов.
    """
    print("Starting full re-indexing process...")
    # Очистка и инициализация индексов
    init_whoosh_indexes()

    counts = {"courses": 0, "materials": 0, "teachers": 0}

    # Индексация курсов
    courses_res = await db.execute(select(DBCourse).options(selectinload(DBCourse.teacher)))
    for course in courses_res.scalars().all():
        index_course_item(
            db_id=course.id, title=course.title, description=course.description,
            category=course.category, level=course.level,
            teacher_name=course.teacher.name if course.teacher else None,
            tags=course.tags
        )
        counts["courses"] += 1

    # Индексация материалов
    materials_res = await db.execute(
        select(DBMaterial).options(selectinload(DBMaterial.course))
    )
    for material in materials_res.scalars().all():
        index_material_item(
            db_id=material.id, title=material.title, content=material.content,
            material_type=material.type, course_id_ref=material.course_id,
            course_title_ref=material.course.title if material.course else "N/A"
        )
        counts["materials"] += 1

    # Индексация преподавателей
    teachers_res = await db.execute(select(DBUser).where(DBUser.role.in_(["teacher", "admin"])))
    for teacher in teachers_res.scalars().all():
        index_teacher_item(db_id=teacher.id, name=teacher.name)
        counts["teachers"] += 1
        
    print(f"Re-indexing complete. Indexed: {counts}")
    return {"status": "success", "indexed_items": counts}

@app.get("/search", response_model=SearchResponse, tags=["Search"])
async def search_api(request: Request, q: Optional[str] = Query(None), category: Optional[str] = Query(None), level: Optional[str] = Query(None), material_type: Optional[str] = Query(None), teacher_name: Optional[str] = Query(None), search_in_courses: bool = Query(True), search_in_materials: bool = Query(True), search_in_teachers: bool = Query(True), limit: int = Query(20, ge=1, le=100)):
    results, raw_facets = search_whoosh(query_str=q, search_in_courses=search_in_courses, search_in_materials=search_in_materials, search_in_teachers=search_in_teachers, filter_category=category, filter_level=level, filter_material_type=material_type, filter_teacher_name=teacher_name, limit=limit)
    facets_model = SearchFacets(categories=[SearchFacetValue(value=k, count=v) for k, v in raw_facets.get("categories", {}).items()], levels=[SearchFacetValue(value=k, count=v) for k, v in raw_facets.get("levels", {}).items()], tags=[SearchFacetValue(value=k, count=v) for k, v in raw_facets.get("tags", {}).items()], material_types=[SearchFacetValue(value=k, count=v) for k, v in raw_facets.get("material_types", {}).items()], teachers=[SearchFacetValue(value=k, count=v) for k, v in raw_facets.get("teachers", {}).items()])
    return SearchResponse(query=q, filters=dict(request.query_params), results=[SearchResultItem.model_validate(item) for item in results], facets=facets_model)

# --- Analytics ---
@app.get("/analytics/user/{user_id}/progress", response_model=List[Dict[str, Any]], tags=["Analytics"])
async def get_user_progress_api(user_id: int, db: AsyncSession = Depends(get_db), current_user: DBUser = Depends(get_current_user)):
    if user_id != current_user.id and current_user.role not in ["admin", "teacher"]: raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Not authorized to view this user's progress.")
    subquery = select(DBMaterial.course_id).join(DBActivity).where(DBActivity.user_id == user_id).distinct().alias("interacted_courses")
    course_material_counts_stmt = select(DBCourse.id, func.count(DBMaterial.id).label("total_materials")).join(DBMaterial).where(DBCourse.id.in_(select(subquery))).group_by(DBCourse.id)
    course_totals = {cid: total for cid, total in (await db.execute(course_material_counts_stmt)).all()}
    stmt = select(DBActivity, DBMaterial.course_id, DBCourse.title).join(DBMaterial).join(DBCourse).where(DBActivity.user_id == user_id)
    course_progress_map: Dict[int, Dict[str, Any]] = {}
    for activity, course_id, course_title in (await db.execute(stmt)).all():
        if course_id not in course_progress_map: course_progress_map[course_id] = {"course_id": course_id, "course_title": course_title, "total_materials": course_totals.get(course_id, 0), "completed_material_ids": set(), "total_time": 0.0, "scores": []}
        progress_item = course_progress_map[course_id]
        if activity.action == "complete": progress_item["completed_material_ids"].add(activity.material_id)
        if activity.duration: progress_item["total_time"] += activity.duration
        if activity.score is not None: progress_item["scores"].append(activity.score)
    response_list = []
    for data_item in course_progress_map.values():
        completed, total = len(data_item["completed_material_ids"]), data_item["total_materials"]
        response_list.append({"course_id": data_item["course_id"], "course_title": data_item["course_title"], "total_materials": total, "completed_materials": completed, "completion_percentage": (completed / total * 100) if total > 0 else 0.0, "total_time": data_item["total_time"], "avg_score": (sum(data_item["scores"]) / len(data_item["scores"])) if data_item["scores"] else None})
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
    for row in all_activities_result.mappings().all():
        key = (row.user_id, row.course_id)
        if key not in user_course_data: 
            user_course_data[key] = {"user_id": row.user_id, "course_id": row.course_id, "user_name": row.user_name, "course_title": row.course_title, 
                                   "completed_material_ids": set(), "total_time_spent_seconds": 0.0, "actions_count": 0, 
                                   "quiz_scores": [], "first_activity_timestamp": row.timestamp, "last_activity_timestamp": row.timestamp}
        agg_data = user_course_data[key]; agg_data["actions_count"] += 1
        if row.duration and row.duration > 0 : agg_data["total_time_spent_seconds"] += row.duration
        if row.action == "complete": agg_data["completed_material_ids"].add(row.material_id)
        if row.material_type == "quiz" and row.score is not None: agg_data["quiz_scores"].append(row.score)
        if row.timestamp > agg_data["last_activity_timestamp"]: agg_data["last_activity_timestamp"] = row.timestamp
    
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
    stmt = (select(DBCourse, DBUser.name.label("teacher_name_alias"), func.count(DBMaterial.id).label("num_materials_alias"))
        .outerjoin(DBUser, DBCourse.teacher_id == DBUser.id).outerjoin(DBMaterial, DBCourse.id == DBMaterial.course_id)
        .group_by(DBCourse.id, DBUser.name).order_by(DBCourse.id))
    result = await db.execute(stmt)
    course_features_list: List[CourseFeatureETL] = []
    for course_row, teacher_name_val, num_materials_val in result.all():
        tag_list = [t.strip().lower() for t in course_row.tags.split(',')] if course_row.tags and course_row.tags.strip() else []
        course_features_list.append(CourseFeatureETL(course_id=course_row.id, title=course_row.title, description=course_row.description, category=course_row.category, level=course_row.level, teacher_id=course_row.teacher_id, teacher_name=teacher_name_val, created_at=course_row.created_at, num_materials=num_materials_val or 0, tags=tag_list))
    return course_features_list

@app.get("/etl/user_features", response_model=List[UserFeatureETL], tags=["ETL & Data Preparation"])
async def etl_get_user_features_api(db: AsyncSession = Depends(get_db), current_user_admin: DBUser = Depends(require_role("admin"))):
    user_course_interactions_list: List[UserCourseInteractionETL] = await etl_get_user_course_interactions_api(db=db, current_user_admin=current_user_admin)
    user_interactions_map: Dict[int, List[UserCourseInteractionETL]] = {}
    for interaction in user_course_interactions_list: user_interactions_map.setdefault(interaction.user_id, []).append(interaction)
    all_courses_info_stmt = select(DBCourse.id, DBCourse.category, DBCourse.level); all_courses_info_res = await db.execute(all_courses_info_stmt)
    courses_meta_map = {row.id: {"category": row.category, "level": row.level} for row in all_courses_info_res.mappings().all()}
    all_users_result = await db.execute(select(DBUser)); users_from_db = all_users_result.scalars().all()
    user_features_list: List[UserFeatureETL] = []
    for db_user in users_from_db:
        interactions = user_interactions_map.get(db_user.id, [])
        total_courses_interacted = len(interactions); total_courses_completed = sum(1 for i in interactions if i.progress_percentage >= 100.0)
        total_time_spent_learning = sum(i.total_time_spent_seconds for i in interactions)
        avg_progress = sum(i.progress_percentage for i in interactions) / len(interactions) if interactions else None
        user_activities_stmt = select(DBActivity.action, DBActivity.score, DBMaterial.type).join(DBMaterial).where(DBActivity.user_id == db_user.id); user_activities_res = await db.execute(user_activities_stmt)
        total_activities_logged = 0; all_quiz_scores = []
        for act_row in user_activities_res.mappings().all():
            total_activities_logged += 1
            if act_row.type == "quiz" and act_row.score is not None: all_quiz_scores.append(act_row.score)
        avg_score_all_quizzes = sum(all_quiz_scores) / len(all_quiz_scores) if all_quiz_scores else None
        category_counter = Counter(); level_counter = Counter()
        for interaction in interactions:
            if interaction.course_id and interaction.progress_percentage > 30:
                course_meta = courses_meta_map.get(interaction.course_id)
                if course_meta: category_counter[course_meta["category"]] +=1; level_counter[course_meta["level"]] +=1
        user_features_list.append(UserFeatureETL(user_id=db_user.id, user_name=db_user.name, user_email=db_user.email, role=db_user.role, created_at=db_user.created_at, is_active=db_user.is_active, total_courses_interacted_with=total_courses_interacted, total_courses_completed=total_courses_completed, total_activities_logged=total_activities_logged, total_time_spent_learning_seconds=round(total_time_spent_learning, 2), avg_progress_on_interacted_courses=round(avg_progress, 2) if avg_progress is not None else None, avg_score_on_all_quizzes=round(avg_score_all_quizzes, 2) if avg_score_all_quizzes is not None else None, preferred_categories=[c for c, _ in category_counter.most_common(3)], preferred_levels=[l for l, _ in level_counter.most_common(3)]))
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