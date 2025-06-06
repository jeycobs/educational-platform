import os
from pathlib import Path
from whoosh import index, qparser as whoosh_qparser 
from whoosh.fields import Schema, ID, TEXT, KEYWORD, STORED 
from whoosh.query import And, Or, Term, Every 
from whoosh.analysis import StandardAnalyzer
from typing import List, Dict, Any, Optional, Tuple

try:
    from whoosh.qparser.common import WildcardNotAllowedError, GtLtNotAllowedError
except ImportError:
    WildcardNotAllowedError = whoosh_qparser.QueryParserError
    GtLtNotAllowedError = whoosh_qparser.QueryParserError

INDEX_DIR_NAME = "whoosh_platform_index"
INDEX_DIR = Path(__file__).resolve().parent / INDEX_DIR_NAME

analyzer = StandardAnalyzer()
print("Using StandardAnalyzer for Whoosh Schemas.")

course_schema = Schema(
    db_id=ID(stored=True, unique=True),
    title=TEXT(stored=True, analyzer=analyzer, field_boost=2.0),
    description=TEXT(analyzer=analyzer, stored=True),
    category=KEYWORD(stored=True, scorable=True, lowercase=True),
    level=KEYWORD(stored=True, scorable=True, lowercase=True),
    teacher_name=TEXT(stored=True, analyzer=analyzer),
    tags=KEYWORD(stored=True, commas=True, scorable=True, lowercase=True) 
)
material_schema = Schema(
    db_id=ID(stored=True, unique=True),
    title=TEXT(stored=True, analyzer=analyzer, field_boost=1.5),
    content=TEXT(analyzer=analyzer, stored=True), 
    material_type=KEYWORD(stored=True, scorable=True, lowercase=True),
    course_id_ref=ID(stored=True), 
    course_title_ref=TEXT(stored=True, analyzer=analyzer),
)
teacher_schema = Schema(
    db_id=ID(stored=True, unique=True),
    name=TEXT(stored=True, analyzer=analyzer, field_boost=2.0),
)

ix_courses: Optional[index.Index] = None
ix_materials: Optional[index.Index] = None
ix_teachers: Optional[index.Index] = None

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
        valid_fields_to_index = {k: v for k, v in fields_to_index.items() if v is not None}
        document_data.update(valid_fields_to_index)
        writer.update_document(**document_data)
        writer.commit()
    except Exception as e:
        print(f"Error indexing item {unique_field_name}={unique_field_value} with data {fields_to_index}: {e}")

def index_course_item(db_id: int, title: str, category: str, level: str, teacher_name: Optional[str], description: Optional[str], tags: Optional[str]):
    _index_item(ix_courses, "db_id", str(db_id),
                  title=title, description=description or "", category=category, level=level, 
                  teacher_name=teacher_name or "", tags=tags or "")

def index_material_item(db_id: int, title: str, material_type: str, course_id_ref: int, course_title_ref: str, content: Optional[str]):
    _index_item(ix_materials, "db_id", str(db_id),
                  title=title, content=content or "", material_type=material_type, 
                  course_id_ref=str(course_id_ref), course_title_ref=course_title_ref)

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
    filter_tags: Optional[List[str]] = None, 
    filter_material_type: Optional[str] = None,
    filter_teacher_name: Optional[str] = None,
    limit: int = 20
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]: 
    if not (ix_courses and ix_materials and ix_teachers):
        print("Whoosh indexes not initialized during search, attempting to initialize now...")
        init_whoosh_indexes()
        if not (ix_courses and ix_materials and ix_teachers):
            print("Error: Whoosh indexes could not be initialized. Search will return empty.")
            return [], {}
            
    all_search_results: List[Dict[str, Any]] = []
    facets_data: Dict[str, Any] = {"categories": {}, "levels": {}, "tags": {}, "material_types": {}, "teachers": {}}

    if search_in_courses and ix_courses:
        try:
            with ix_courses.searcher() as searcher:
                parser_courses = whoosh_qparser.MultifieldParser(
                    ["title", "description", "teacher_name", "tags", "category", "level"], 
                    schema=ix_courses.schema, 
                    group=whoosh_qparser.OrGroup
                )
                course_query_parts = []
                if query_str:
                    try: course_query_parts.append(parser_courses.parse(query_str))
                    except whoosh_qparser.QueryParserError: course_query_parts.append(parser_courses.parse(whoosh_qparser.QueryParser.escape(query_str)))
                if filter_category: course_query_parts.append(Term("category", filter_category.lower()))
                if filter_level: course_query_parts.append(Term("level", filter_level.lower()))
                if filter_teacher_name: course_query_parts.append(whoosh_qparser.QueryParser("teacher_name", ix_courses.schema).parse(filter_teacher_name))
                if filter_tags:
                    tag_queries = [Term("tags", tag.strip().lower()) for tag in filter_tags if tag.strip()]
                    if tag_queries: course_query_parts.append(Or(tag_queries))

                final_course_query = None
                if course_query_parts:
                    final_course_query = And(course_query_parts) if len(course_query_parts) > 1 else course_query_parts[0]
                elif not any([query_str, filter_category, filter_level, filter_teacher_name, filter_tags]):
                    final_course_query = Every()
                
                if final_course_query:
                    results_courses = searcher.search(final_course_query, limit=limit, groupedby=["category", "level", "tags"])
                    
                    if "category" in results_courses.facet_names():
                        facets_data["categories"] = {
                            term: len(doc_ids) for term, doc_ids in results_courses.groups("category").items()
                        }
                    if "level" in results_courses.facet_names():
                        facets_data["levels"] = {
                            term: len(doc_ids) for term, doc_ids in results_courses.groups("level").items()
                        }
                    if "tags" in results_courses.facet_names():
                        facets_data["tags"] = {
                            term: len(doc_ids) for term, doc_ids in results_courses.groups("tags").items()
                        }
                    
                    for hit in results_courses:
                        all_search_results.append({"id": int(hit["db_id"]), "title": hit["title"], "type": "course", "description": hit.get("description"), "category": hit.get("category"), "level": hit.get("level"), "teacher_name": hit.get("teacher_name"), "tags": hit.get("tags", "").split(',') if hit.get("tags") else [], "relevance_score": hit.score})
        except Exception as e:
            print(f"Error during course search or faceting: {e}")
            import traceback; traceback.print_exc()

    if search_in_materials and ix_materials:
        try:
            with ix_materials.searcher() as searcher:
                parser_materials = whoosh_qparser.MultifieldParser(["title", "content", "course_title_ref"], schema=ix_materials.schema, group=whoosh_qparser.OrGroup)
                material_query_parts = []
                if query_str:
                    try: material_query_parts.append(parser_materials.parse(query_str))
                    except whoosh_qparser.QueryParserError: material_query_parts.append(parser_materials.parse(whoosh_qparser.QueryParser.escape(query_str)))
                if filter_material_type: material_query_parts.append(Term("material_type", filter_material_type.lower()))

                final_material_query = None
                if material_query_parts:
                    final_material_query = And(material_query_parts) if len(material_query_parts) > 1 else material_query_parts[0]
                elif not any([query_str, filter_material_type]):
                     final_material_query = Every()
                
                if final_material_query:
                    results_materials = searcher.search(final_material_query, limit=limit, groupedby=["material_type"])
                    
                    if "material_type" in results_materials.facet_names():
                        facets_data["material_types"] = {
                            term: len(doc_ids) for term, doc_ids in results_materials.groups("material_type").items()
                        }
                    
                    for hit in results_materials:
                        all_search_results.append({"id": int(hit["db_id"]), "title": f"{hit['title']} (Курс: {hit.get('course_title_ref', 'N/A')})", "type": "material", "description": hit.get("content"), "material_type_field": hit.get("material_type"), "relevance_score": hit.score})
        except Exception as e:
            print(f"Error during material search or faceting: {e}")
            import traceback; traceback.print_exc()

    if search_in_teachers and ix_teachers:
        try:
            with ix_teachers.searcher() as searcher:
                effective_teacher_query_str = filter_teacher_name or query_str
                final_teacher_query = None
                if effective_teacher_query_str:
                    parser_teachers = whoosh_qparser.QueryParser("name", schema=ix_teachers.schema, group=whoosh_qparser.OrGroup)
                    try: final_teacher_query = parser_teachers.parse(effective_teacher_query_str)
                    except whoosh_qparser.QueryParserError: final_teacher_query = parser_teachers.parse(whoosh_qparser.QueryParser.escape(effective_teacher_query_str))
                elif not any([filter_teacher_name, query_str]): 
                    final_teacher_query = Every() 
                
                if final_teacher_query:
                    results_teachers = searcher.search(final_teacher_query, limit=limit)
                    for hit in results_teachers:
                         all_search_results.append({"id": int(hit["db_id"]), "title": hit["name"], "type": "teacher", "relevance_score": hit.score})

        except Exception as e:
            print(f"Error during teacher search: {e}")
            import traceback; traceback.print_exc()

    all_search_results.sort(key=lambda x: x.get("relevance_score", 0), reverse=True)
    final_facets = {k: v for k, v in facets_data.items() if v}
    return all_search_results[:limit], final_facets