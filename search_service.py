# search_service.py
import os
from pathlib import Path
from whoosh import index
from whoosh.fields import Schema, ID, TEXT, KEYWORD
from whoosh.qparser import MultifieldParser, QueryParser, OrGroup as QParserOrGroup, AndGroup as QParserAndGroup, QueryParserError # Переименовал для ясности
from whoosh.query import And, Or, Term # <--- ДОБАВИТЬ ИМПОРТ And, Or
from whoosh.analysis import StandardAnalyzer
from typing import List, Dict, Any, Optional

try:
    from whoosh.qparser.common import WildcardNotAllowedError, GtLtNotAllowedError
except ImportError:
    WildcardNotAllowedError = QueryParserError 
    GtLtNotAllowedError = QueryParserError

INDEX_DIR_NAME = "whoosh_platform_index"
INDEX_DIR = Path(__file__).resolve().parent / INDEX_DIR_NAME

russian_analyzer = StandardAnalyzer()
print("Using StandardAnalyzer for Whoosh Schemas (language-neutral tokenization and lowercasing).")

course_schema = Schema( # ... (схемы остаются такими же) ...
    db_id=ID(stored=True, unique=True),
    title=TEXT(stored=True, analyzer=russian_analyzer, field_boost=2.0),
    category=KEYWORD(stored=True, scorable=True, lowercase=True),
    level=KEYWORD(stored=True, scorable=True, lowercase=True),
    teacher_name=TEXT(stored=True, analyzer=russian_analyzer),
)
material_schema = Schema( # ... (схемы остаются такими же) ...
    db_id=ID(stored=True, unique=True),
    title=TEXT(stored=True, analyzer=russian_analyzer, field_boost=1.5),
    material_type=KEYWORD(stored=True, scorable=True, lowercase=True),
    course_id_ref=ID(stored=True),
    course_title_ref=TEXT(stored=True, analyzer=russian_analyzer),
)
teacher_schema = Schema( # ... (схемы остаются такими же) ...
    db_id=ID(stored=True, unique=True),
    name=TEXT(stored=True, analyzer=russian_analyzer, field_boost=2.0),
)


ix_courses: Optional[index.Index] = None
ix_materials: Optional[index.Index] = None
ix_teachers: Optional[index.Index] = None

# ... (функции get_or_create_index, init_whoosh_indexes, _index_item, index_*, delete_* остаются такими же) ...
def get_or_create_index(index_path_str: str, schema_obj: Schema) -> index.Index:
    index_path = Path(index_path_str)
    if not index_path.exists():
        index_path.mkdir(parents=True, exist_ok=True)
        print(f"Creating Whoosh index at: {index_path_str}")
        return index.create_in(str(index_path), schema_obj)
    try:
        print(f"Opening Whoosh index at: {index_path_str}")
        return index.open_dir(str(index_path))
    except index.EmptyIndexError:
        print(f"Whoosh index at {index_path_str} is empty or corrupted. Recreating.")
        return index.create_in(str(index_path), schema_obj)

def init_whoosh_indexes():
    global ix_courses, ix_materials, ix_teachers
    if not INDEX_DIR.exists():
        INDEX_DIR.mkdir(parents=True, exist_ok=True)
    
    print(f"Whoosh index directory: {INDEX_DIR.resolve()}")
    ix_courses = get_or_create_index(str(INDEX_DIR / "courses"), course_schema)
    ix_materials = get_or_create_index(str(INDEX_DIR / "materials"), material_schema)
    ix_teachers = get_or_create_index(str(INDEX_DIR / "teachers"), teacher_schema)
    print("Whoosh indexes initialized/opened.")

def _index_item(index_obj: Optional[index.Index], unique_field_name: str, unique_field_value: str, **fields_to_index):
    if not index_obj:
        print(f"Warning: Index object is None for {unique_field_name}={unique_field_value}. Cannot index item.")
        return
    try:
        writer = index_obj.writer()
        document_data = {unique_field_name: str(unique_field_value)}
        document_data.update(fields_to_index)
        writer.update_document(**document_data)
        writer.commit()
    except Exception as e:
        print(f"Error indexing item {unique_field_name}={unique_field_value}: {e}")


def index_course_item(db_id: int, title: str, category: str, level: str, teacher_name: Optional[str]):
    _index_item(ix_courses, "db_id", str(db_id),
                  title=title, category=category, level=level, teacher_name=teacher_name or "")

def index_material_item(db_id: int, title: str, material_type: str, course_id_ref: int, course_title_ref: str):
    _index_item(ix_materials, "db_id", str(db_id),
                  title=title, material_type=material_type, course_id_ref=str(course_id_ref), course_title_ref=course_title_ref)

def index_teacher_item(db_id: int, name: str):
    _index_item(ix_teachers, "db_id", str(db_id), name=name)

def delete_item_from_index(index_obj: Optional[index.Index], db_id: int):
    if not index_obj:
        print(f"Warning: Index object is None. Cannot delete item {db_id}.")
        return
    try:
        writer = index_obj.writer()
        writer.delete_by_term('db_id', str(db_id))
        writer.commit()
    except Exception as e:
        print(f"Error deleting item {db_id} from index: {e}")


def search_whoosh(
    query_str: Optional[str],
    search_in_courses: bool = True,
    search_in_materials: bool = True,
    search_in_teachers: bool = True,
    filter_category: Optional[str] = None,
    filter_level: Optional[str] = None,
    filter_material_type: Optional[str] = None,
    filter_teacher_name: Optional[str] = None,
    limit: int = 20
) -> List[Dict[str, Any]]:
    if not (ix_courses and ix_materials and ix_teachers):
        print("Whoosh indexes not initialized, attempting to initialize now...")
        init_whoosh_indexes()
        if not (ix_courses and ix_materials and ix_teachers):
            print("Error: Whoosh indexes could not be initialized. Search will return empty.")
            return []
            
    all_search_results: List[Dict[str, Any]] = []
    print(f"\n--- DEBUG Whoosh Search ---")
    print(f"Query String: '{query_str}'")
    print(f"Filters: Category='{filter_category}', Level='{filter_level}', MaterialType='{filter_material_type}', TeacherName='{filter_teacher_name}'")

    # --- Поиск по Курсам ---
    if search_in_courses and ix_courses:
        print("Searching in courses...")
        try:
            with ix_courses.searcher() as searcher:
                parser_courses = MultifieldParser(["title", "teacher_name"], schema=ix_courses.schema, group=QParserOrGroup) # Используем QParserOrGroup
                active_filters_courses = []
                if filter_category:
                    # QueryParser().parse() возвращает объект запроса, который можно использовать с query.And/Or
                    active_filters_courses.append(QueryParser("category", ix_courses.schema).parse(filter_category.lower()))
                if filter_level:
                    active_filters_courses.append(QueryParser("level", ix_courses.schema).parse(filter_level.lower()))
                if filter_teacher_name:
                     active_filters_courses.append(QueryParser("teacher_name", ix_courses.schema).parse(filter_teacher_name))
                
                text_query_courses = None
                if query_str:
                    try:
                        text_query_courses = parser_courses.parse(query_str)
                    except (WildcardNotAllowedError, GtLtNotAllowedError, QueryParserError) as e: 
                        error_type_msg = "Specific parsing error"
                        if isinstance(e, QueryParserError) and not isinstance(e, (WildcardNotAllowedError, GtLtNotAllowedError)):
                            error_type_msg = "General QueryParserError"
                        print(f"{error_type_msg} for courses with query: '{query_str}'. Error: {type(e).__name__}. Escaping.")
                        text_query_courses = parser_courses.parse(QueryParser.escape(query_str))
                
                final_query_parts_courses = []
                if text_query_courses: final_query_parts_courses.append(text_query_courses)
                if active_filters_courses: final_query_parts_courses.extend(active_filters_courses)

                if final_query_parts_courses:
                    # Используем whoosh.query.And для объединения
                    if len(final_query_parts_courses) > 1:
                        final_query_obj_courses = And(final_query_parts_courses)
                    else:
                        final_query_obj_courses = final_query_parts_courses[0]
                    
                    print(f"Final Whoosh query for courses: {final_query_obj_courses}")
                    results_courses = searcher.search(final_query_obj_courses, limit=limit)
                    print(f"Found {len(results_courses)} course(s) in Whoosh.")
                    for i, hit in enumerate(results_courses):
                        print(f"  Course Hit {i+1}: ID={hit['db_id']}, Title='{hit['title']}', Score={hit.score}, StoredFields={dict(hit)}")
                        all_search_results.append({
                            "id": int(hit["db_id"]), "title": hit["title"], "type": "course",
                            "category": hit.get("category"), "level": hit.get("level"),
                            "teacher_name": hit.get("teacher_name"), "relevance_score": hit.score
                        })
                else:
                    print("No query parts for courses, skipping search.")
        except Exception as e:
            print(f"Error during course search: {e}")
            import traceback
            traceback.print_exc()

    # --- Поиск по Материалам ---
    if search_in_materials and ix_materials:
        print("Searching in materials...")
        try:
            with ix_materials.searcher() as searcher:
                parser_materials = MultifieldParser(["title", "course_title_ref"], schema=ix_materials.schema, group=QParserOrGroup)
                active_filters_materials = []
                if filter_material_type:
                    active_filters_materials.append(QueryParser("material_type", ix_materials.schema).parse(filter_material_type.lower()))
                
                text_query_materials = None
                if query_str:
                    try:
                        text_query_materials = parser_materials.parse(query_str)
                    except (WildcardNotAllowedError, GtLtNotAllowedError, QueryParserError) as e:
                        error_type_msg = "Specific parsing error"
                        if isinstance(e, QueryParserError) and not isinstance(e, (WildcardNotAllowedError, GtLtNotAllowedError)):
                            error_type_msg = "General QueryParserError"
                        print(f"{error_type_msg} for materials with query: '{query_str}'. Error: {type(e).__name__}. Escaping.")
                        text_query_materials = parser_materials.parse(QueryParser.escape(query_str))
                
                final_query_parts_materials = []
                if text_query_materials: final_query_parts_materials.append(text_query_materials)
                if active_filters_materials: final_query_parts_materials.extend(active_filters_materials)

                if final_query_parts_materials:
                    if len(final_query_parts_materials) > 1:
                        final_query_obj_materials = And(final_query_parts_materials)
                    else:
                        final_query_obj_materials = final_query_parts_materials[0]

                    print(f"Final Whoosh query for materials: {final_query_obj_materials}")
                    results_materials = searcher.search(final_query_obj_materials, limit=limit)
                    print(f"Found {len(results_materials)} material(s) in Whoosh.")
                    for i, hit in enumerate(results_materials):
                        print(f"  Material Hit {i+1}: ID={hit['db_id']}, Title='{hit['title']}', Score={hit.score}, StoredFields={dict(hit)}")
                        all_search_results.append({
                            "id": int(hit["db_id"]), "title": f"{hit['title']} (из курса: {hit.get('course_title_ref', 'N/A')})",
                            "type": "material", "material_type_field": hit.get("material_type"),
                            "relevance_score": hit.score
                        })
                else:
                    print("No query parts for materials, skipping search.")
        except Exception as e:
            print(f"Error during material search: {e}")
            import traceback
            traceback.print_exc()


    # --- Поиск по Преподавателям ---
    if search_in_teachers and ix_teachers:
        print("Searching in teachers...")
        try:
            with ix_teachers.searcher() as searcher:
                teacher_query_text = filter_teacher_name or query_str
                final_query_obj_teachers = None # Инициализируем
                if teacher_query_text:
                    parser_teachers = QueryParser("name", schema=ix_teachers.schema, group=QParserOrGroup)
                    try:
                        final_query_obj_teachers = parser_teachers.parse(teacher_query_text) # Это уже объект запроса
                    except (WildcardNotAllowedError, GtLtNotAllowedError, QueryParserError) as e:
                        error_type_msg = "Specific parsing error"
                        if isinstance(e, QueryParserError) and not isinstance(e, (WildcardNotAllowedError, GtLtNotAllowedError)):
                            error_type_msg = "General QueryParserError"
                        print(f"{error_type_msg} for teachers with query: '{teacher_query_text}'. Error: {type(e).__name__}. Escaping.")
                        final_query_obj_teachers = parser_teachers.parse(QueryParser.escape(teacher_query_text))
                    
                    if final_query_obj_teachers: # Проверяем, что запрос сформирован
                        print(f"Final Whoosh query for teachers: {final_query_obj_teachers}")
                        results_teachers = searcher.search(final_query_obj_teachers, limit=limit)
                        print(f"Found {len(results_teachers)} teacher(s) in Whoosh.")
                        for i, hit in enumerate(results_teachers):
                            print(f"  Teacher Hit {i+1}: ID={hit['db_id']}, Name='{hit['name']}', Score={hit.score}, StoredFields={dict(hit)}")
                            all_search_results.append({
                                "id": int(hit["db_id"]), "title": hit["name"],
                                "type": "teacher", "relevance_score": hit.score
                            })
                else:
                    print("No query text for teachers, skipping search.")
        except Exception as e:
            print(f"Error during teacher search: {e}")
            import traceback
            traceback.print_exc()

    all_search_results.sort(key=lambda x: x.get("relevance_score", 0), reverse=True)
    print(f"Total results before final limit: {len(all_search_results)}")
    print(f"--- END DEBUG Whoosh Search ---\n")
    return all_search_results[:limit]