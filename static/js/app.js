// static/js/app.js
class EduPlatformAPI {
    constructor() {
        this.baseURL = ''; // Пусто, так как запросы идут на тот же домен
        this.token = localStorage.getItem('access_token');
        this.currentUser = null;
        // this.init(); // Вызовем init после загрузки DOM
    }

    async init() {
        if (this.token) {
            await this.loadCurrentUser();
        }
        this.updateNavigation();
    }

    async request(endpoint, options = {}) {
        const url = this.baseURL + endpoint;
        const config = {
            headers: {
                'Content-Type': 'application/json',
                ...options.headers
            },
            ...options
        };

        if (this.token) {
            config.headers['Authorization'] = `Bearer ${this.token}`;
        }

        try {
            const response = await fetch(url, config);
            
            if (response.status === 401 && endpoint !== '/token') { // Не выходить из системы при ошибке входа
                this.logout(); // Если это не ошибка входа, то выходим
                // window.location.href = '/'; // Перенаправляем на главную
                showAlert('Сессия истекла или недействительна. Пожалуйста, войдите снова.', 'warning');
                throw new Error('Unauthorized');
            }

            const responseData = await response.json();
            if (!response.ok) {
                throw new Error(responseData.detail || 'Request failed with status ' + response.status);
            }
            return responseData;

        } catch (error) {
            console.error(`API request to ${endpoint} failed:`, error);
            // showAlert(error.message, 'danger'); // Не показываем alert для каждого запроса, только для действий пользователя
            throw error;
        }
    }

    async login(email, password) { // email вместо username
        const formData = new FormData();
        formData.append('username', email); // FastAPI OAuth2PasswordRequestForm ожидает 'username'
        formData.append('password', password);

        // Используем fetch напрямую, так как this.request может вызвать logout при 401
        const response = await fetch('/token', {
            method: 'POST',
            body: formData
        });
        
        const data = await response.json();
        if (!response.ok) {
            throw new Error(data.detail || 'Invalid credentials');
        }

        this.token = data.access_token;
        localStorage.setItem('access_token', this.token);
        
        await this.loadCurrentUser();
        this.updateNavigation();
        return data;
    }

    async register(userData) {
        // URL изменен на /users/register
        return await this.request('/users/register', {
            method: 'POST',
            body: JSON.stringify(userData)
        });
    }

    logout() {
        this.token = null;
        this.currentUser = null;
        localStorage.removeItem('access_token');
        this.updateNavigation();
        if (window.location.pathname !== '/') { // Не перенаправлять, если уже на главной
            window.location.href = '/';
        }
    }

    async loadCurrentUser() {
        if (!this.token) return;
        try {
            this.currentUser = await this.request('/users/me');
        } catch (error) {
            // Ошибка уже обрабатывается в this.request (logout при 401)
            console.error("Failed to load current user, token might be invalid.");
            this.token = null; // Очищаем невалидный токен
            localStorage.removeItem('access_token');
        }
    }

    updateNavigation() {
        const authNav = document.getElementById('authNav');
        const dashboardLink = document.querySelector('a[href="/dashboard"]'); // Ссылка на дашборд
        
        if (!authNav) return;

        if (this.currentUser) {
            authNav.innerHTML = `
                <li class="nav-item dropdown">
                    <a class="nav-link dropdown-toggle text-white" href="#" id="navbarDropdownUserMenu" role="button" data-bs-toggle="dropdown" aria-expanded="false">
                        <i class="bi bi-person-circle me-1"></i>${this.currentUser.name}
                    </a>
                    <ul class="dropdown-menu dropdown-menu-end" aria-labelledby="navbarDropdownUserMenu">
                        <li><a class="dropdown-item" href="/dashboard">
                            <i class="bi bi-grid me-2"></i>Панель управления
                        </a></li>
                        <li><hr class="dropdown-divider"></li>
                        <li><a class="dropdown-item" href="#" id="logoutButton">
                            <i class="bi bi-box-arrow-right me-2"></i>Выйти
                        </a></li>
                    </ul>
                </li>
            `;
            // Добавляем обработчик для кнопки выхода, так как innerHTML его сбрасывает
            document.getElementById('logoutButton').addEventListener('click', (e) => {
                e.preventDefault();
                this.logout();
            });
            if (dashboardLink) dashboardLink.style.display = 'block';
        } else {
            authNav.innerHTML = `
                <li class="nav-item">
                    <a class="nav-link text-white" href="#" onclick="showAuthModal()">
                        <i class="bi bi-box-arrow-in-right me-1"></i>Войти / Регистрация
                    </a>
                </li>
            `;
            if (dashboardLink) dashboardLink.style.display = 'none';
        }
    }

    async getCourses(filters = {}) {
        const params = new URLSearchParams(filters);
        return await this.request(`/courses?${params.toString()}`);
    }
    
    async getCourseMaterials(courseId) {
        return await this.request(`/courses/${courseId}/materials`);
    }


    async createCourse(courseData) {
        return await this.request('/courses', {
            method: 'POST',
            body: JSON.stringify(courseData)
        });
    }

    async search(params = {}) { // Принимает объект параметров
        const queryParams = new URLSearchParams();
        for (const key in params) {
            if (params[key] !== null && params[key] !== undefined && params[key] !== '') {
                if (typeof params[key] === 'boolean') {
                    queryParams.append(key, params[key].toString());
                } else {
                    queryParams.append(key, params[key]);
                }
            }
        }
        return await this.request(`/search?${queryParams.toString()}`);
    }

    async getUserProgress(userId) { // userId здесь не нужен, так как current_user уже есть
        if (!this.currentUser) throw new Error("User not authenticated");
        return await this.request(`/analytics/user/${this.currentUser.id}/progress`);
    }

    async logActivity(activityData) {
        // user_id должен быть ID текущего пользователя
        if (!this.currentUser) throw new Error("User not authenticated for logging activity");
        const dataToLog = {
            ...activityData,
            user_id: this.currentUser.id 
        };
        // Эндпоинт из объединенного main.py - /activity/log_event
        return await this.request('/activity/log_event', {
            method: 'POST',
            body: JSON.stringify(dataToLog)
        });
    }
}

// Global API instance
const api = new EduPlatformAPI();

document.addEventListener('DOMContentLoaded', () => {
    api.init(); // Инициализируем API после загрузки DOM

    // Обработчики форм (если они есть на странице)
    const loginForm = document.getElementById('loginForm');
    if (loginForm) loginForm.addEventListener('submit', handleLogin);

    const registerFormEl = document.getElementById('registerForm'); // Используем новое имя id из base.html
    if (registerFormEl) registerFormEl.addEventListener('submit', handleRegister);
    
    const searchFormModal = document.getElementById('searchFormModal'); // Для модального окна поиска
    if (searchFormModal) searchFormModal.addEventListener('submit', handleSearch);

    const createCourseForm = document.getElementById('createCourseForm');
    if (createCourseForm) createCourseForm.addEventListener('submit', handleCreateCourse);

    // Инициализация для конкретных страниц
    if (window.location.pathname === '/') {
        loadFeaturedCourses();
        // loadStatistics(); // Эта функция была в примере, но эндпоинта для нее нет
    } else if (window.location.pathname === '/dashboard') {
        checkAuthAndLoadDashboard();
    }
});


// --- Authentication Modal Functions ---
function showAuthModal() {
    const authModalEl = document.getElementById('authModal');
    if (authModalEl) {
        const modal = bootstrap.Modal.getOrCreateInstance(authModalEl);
        modal.show();
        showLogin(); // По умолчанию показываем логин
    }
}

function showLogin() {
    const loginFormEl = document.getElementById('loginFormContent'); // Новые ID из base.html
    const registerFormEl = document.getElementById('registerFormContent');
    const authModalTitleEl = document.getElementById('authModalTitle');

    if (loginFormEl) loginFormEl.style.display = 'block';
    if (registerFormEl) registerFormEl.style.display = 'none';
    if (authModalTitleEl) authModalTitleEl.textContent = 'Вход в систему';
    updateAuthTabs('login');
}

function showRegister() {
    const loginFormEl = document.getElementById('loginFormContent');
    const registerFormEl = document.getElementById('registerFormContent');
    const authModalTitleEl = document.getElementById('authModalTitle');

    if (loginFormEl) loginFormEl.style.display = 'none';
    if (registerFormEl) registerFormEl.style.display = 'block';
    if (authModalTitleEl) authModalTitleEl.textContent = 'Регистрация';
    updateAuthTabs('register');
}

function updateAuthTabs(active) {
    const tabs = document.querySelectorAll('#authTabs .nav-link');
    tabs.forEach(tab => tab.classList.remove('active'));
    
    const activeTabEl = document.querySelector(`#authTabs .nav-link[onclick="show${active.charAt(0).toUpperCase() + active.slice(1)}()"]`);
    if (activeTabEl) activeTabEl.classList.add('active');
}

async function handleLogin(event) {
    event.preventDefault();
    const form = event.target;
    const email = form.username.value; // 'username' - это name поля email в форме
    const password = form.password.value;
    const loginMsgEl = document.getElementById('loginMsg'); // Для сообщений об ошибках
    if(loginMsgEl) loginMsgEl.textContent = '';


    try {
        await api.login(email, password);
        const authModalEl = document.getElementById('authModal');
        if (authModalEl) bootstrap.Modal.getInstance(authModalEl).hide();
        showAlert('Успешный вход в систему!', 'success');
        
        if (window.location.pathname === '/') {
            window.location.href = '/dashboard';
        } else {
            // window.location.reload(); // Перезагрузка может быть не всегда нужна
        }
    } catch (error) {
        if(loginMsgEl) loginMsgEl.textContent = error.message;
        else showAlert(error.message, 'danger');
    }
}

async function handleRegister(event) {
    event.preventDefault();
    const form = event.target;
    const registerMsgEl = document.getElementById('registerMsg'); // Для сообщений об ошибках
    if(registerMsgEl) registerMsgEl.textContent = '';

    const userData = {
        name: form.name.value,
        email: form.email.value, // Используем email
        role: form.role.value,
        password: form.password.value
    };
    
    try {
        await api.register(userData);
        showAlert('Регистрация успешна! Теперь вы можете войти в систему.', 'success');
        showLogin(); // Показать форму логина после успешной регистрации
        // Очистка полей формы регистрации
        form.reset();
    } catch (error) {
         if(registerMsgEl) registerMsgEl.textContent = error.message;
         else showAlert(error.message, 'danger');
    }
}

// --- Search Modal Functions ---
function showSearchModal() {
    const searchModalEl = document.getElementById('searchModal');
    if (searchModalEl) {
       const modal = bootstrap.Modal.getOrCreateInstance(searchModalEl);
       modal.show();
    }
}

async function handleSearch(event) { // Эта функция привязана к форме в модалке поиска
    event.preventDefault();
    const form = event.target;
    const searchResultsEl = document.getElementById('searchResults');
    if(searchResultsEl) searchResultsEl.innerHTML = '<div class="loading"></div>';

    const params = {
        query: form.q.value, // Используем 'q' как в HTML форме
        category: form.category.value,
        level: form.level.value,
        // Добавим чекбоксы, если они есть в форме модального окна
        // search_in_courses: form.search_in_courses ? form.search_in_courses.checked : true,
        // search_in_materials: form.search_in_materials ? form.search_in_materials.checked : true,
        // search_in_teachers: form.search_in_teachers ? form.search_in_teachers.checked : true,
        // teacher_name: form.teacher_name ? form.teacher_name.value : null, // Если есть поле для имени преподавателя
        // material_type: form.material_type ? form.material_type.value : null // Если есть поле для типа материала
    };
     // Для Whoosh-поиска, который ожидает query, а не q:
    if (params.q) {
        params.query = params.q;
        delete params.q;
    }

    // Добавляем параметры search_in_*, если они есть на форме (для старого HTML)
    // В новом HTML (base.html) таких чекбоксов нет в модалке, можно их добавить или убрать эту логику
    const searchCoursesEl = document.getElementById('search-courses'); // ID из старого HTML
    if (searchCoursesEl) params.search_in_courses = searchCoursesEl.checked;
    const searchMaterialsEl = document.getElementById('search-materials');
    if (searchMaterialsEl) params.search_in_materials = searchMaterialsEl.checked;
    const searchTeachersEl = document.getElementById('search-teachers');
    if (searchTeachersEl) params.search_in_teachers = searchTeachersEl.checked;


    try {
        const response = await api.search(params); // response теперь SearchResponse
        displaySearchResults(response.results); // Передаем только список результатов
    } catch (error) {
        if(searchResultsEl) searchResultsEl.innerHTML = `<div class="alert alert-danger">${error.message}</div>`;
        else showAlert(error.message, 'danger');
    }
}

function displaySearchResults(results) { // results - это список SearchResultItem
    const container = document.getElementById('searchResults');
    if (!container) return;

    if (!results || results.length === 0) {
        container.innerHTML = '<div class="alert alert-info mt-3">Ничего не найдено по вашему запросу.</div>';
        return;
    }
    
    let html = '<h5 class="mt-3">Результаты поиска:</h5>';
    results.forEach(item => {
        html += `
            <div class="card mb-3 course-card">
                <div class="card-body">
                    <h6 class="card-title mb-1">${item.title} 
                        <span class="badge bg-secondary float-end">${item.type}</span>
                    </h6>
                    ${item.category ? `<span class="badge bg-info me-1">${item.category}</span>` : ''}
                    ${item.level ? `<span class="badge bg-light text-dark me-1">${item.level}</span>` : ''}
                    ${item.teacher_name ? `<small class="text-muted d-block mt-1">Преподаватель: ${item.teacher_name}</small>` : ''}
                    ${item.material_type_field ? `<span class="badge bg-warning text-dark me-1">${item.material_type_field}</span>` : ''}
                    ${item.description ? `<p class="card-text small mt-2">${item.description}</p>` : ''}
                    ${item.relevance_score ? `<small class="text-muted">Релевантность: ${item.relevance_score.toFixed(2)}</small>`: ''}
                </div>
            </div>
        `;
    });
    container.innerHTML = html;
}

// --- Main page functions (из нового app.js) ---
async function loadFeaturedCourses() {
    const container = document.getElementById('coursesContainer');
    if (!container) return;
    container.innerHTML = '<div class="loading"></div>';
    try {
        // API getCourses может принимать limit
        const coursesData = await api.getCourses({ limit: 6 }); // coursesData - это List[CourseInDB]
        displayCourses(coursesData, 'coursesContainer');
    } catch (error) {
        container.innerHTML = '<div class="alert alert-danger">Не удалось загрузить курсы.</div>';
        console.error('Failed to load courses:', error);
    }
}

function displayCourses(courses, containerId) { // courses - List[CourseInDB]
    const container = document.getElementById(containerId);
    if (!container) return;
    
    if (!courses || courses.length === 0) {
        container.innerHTML = '<div class="col-12"><div class="alert alert-info">Курсы не найдены.</div></div>';
        return;
    }
    
    container.innerHTML = courses.map(course => `
        <div class="col-md-6 col-lg-4 mb-4">
            <div class="card course-card h-100">
                <div class="card-body d-flex flex-column">
                    <h5 class="card-title">${course.title}</h5>
                    <p class="card-text small text-muted flex-grow-1">${course.description || 'Описание отсутствует.'}</p>
                    <div class="mt-auto">
                        <span class="badge bg-primary me-1">${course.category}</span>
                        <span class="badge bg-secondary">${course.level}</span>
                    </div>
                     <!-- <button class="btn btn-sm btn-outline-primary mt-2" onclick="viewCourseDetails(${course.id})">Подробнее</button> -->
                </div>
            </div>
        </div>
    `).join('');
}

// --- Dashboard functions (из нового app.js) ---
async function checkAuthAndLoadDashboard() {
    if (!api.token) { // Проверяем токен, а не currentUser, т.к. он может быть еще не загружен
        showAlert('Для доступа к панели управления необходимо войти.', 'warning');
        setTimeout(() => { window.location.href = '/'; }, 2000);
        return;
    }
    if (!api.currentUser) { // Если токен есть, но юзер не загружен, пытаемся загрузить
        await api.loadCurrentUser();
        if(!api.currentUser) { // Если и после этого нет, то проблема
             showAlert('Не удалось загрузить данные пользователя. Попробуйте войти снова.', 'danger');
             api.logout(); // Сбрасываем, т.к. токен невалидный
             return;
        }
    }
    
    const createCourseBtn = document.getElementById('createCourseBtn');
    const analyticsTabLink = document.getElementById('analyticsTabLink'); // ID для ссылки на вкладку
    
    if (createCourseBtn && (api.currentUser.role === 'teacher' || api.currentUser.role === 'admin')) {
        createCourseBtn.style.display = 'inline-block'; // или 'block'
    } else if (createCourseBtn) {
        createCourseBtn.style.display = 'none';
    }

    if (analyticsTabLink && (api.currentUser.role === 'teacher' || api.currentUser.role === 'admin')) {
        analyticsTabLink.style.display = 'list-item'; // или 'block' в зависимости от разметки
    } else if (analyticsTabLink) {
        analyticsTabLink.style.display = 'none';
    }
    
    loadUserProfile();
    loadMyCourses(); // Загружаем курсы по умолчанию
    
    // Привязываем обработчики к табам
    const dashboardTabs = document.querySelectorAll('#dashboardTabs .nav-link');
    dashboardTabs.forEach(tab => {
        tab.addEventListener('click', function(event) {
            event.preventDefault();
            const tabId = this.getAttribute('href').substring(1); // Получаем 'my-courses', 'progress', etc.
            showDashboardTab(tabId);
        });
    });
    // Показываем первую вкладку по умолчанию
    const firstTab = document.querySelector('#dashboardTabs .nav-link');
    if (firstTab) {
        new bootstrap.Tab(firstTab).show(); // Активируем первую вкладку Bootstrap
        showDashboardTab(firstTab.getAttribute('href').substring(1));
    }
}

function showDashboardTab(tabId) {
    // Скрываем все панели
    document.querySelectorAll('.dashboard-tab-pane').forEach(pane => {
        pane.classList.remove('show', 'active');
    });
    // Показываем нужную панель
    const activePane = document.getElementById(tabId);
    if (activePane) {
        activePane.classList.add('show', 'active');
    }

    // Загружаем контент для активной вкладки, если он еще не загружен или требует обновления
    if (tabId === 'my-courses-pane') {
        loadMyCourses();
    } else if (tabId === 'progress-pane') {
        loadUserProgress();
    } else if (tabId === 'activity-pane') {
        loadUserActivity();
    } else if (tabId === 'settings-pane') {
        // loadUserSettings();
    }
}


async function loadUserProfile() {
    const container = document.getElementById('profileInfoContainer'); // Новый ID из dashboard.html
    if (!container || !api.currentUser) return;
    
    container.innerHTML = `
        <div class="text-center mb-3">
            <i class="bi bi-person-circle display-3 text-primary"></i>
        </div>
        <h5 class="card-title text-center">${api.currentUser.name}</h5>
        <p class="card-text text-center text-muted mb-1">${api.currentUser.email}</p>
        <p class="text-center"><span class="badge bg-info">${api.currentUser.role}</span></p>
        <hr>
        <p class="card-text"><small class="text-muted">Зарегистрирован: ${new Date(api.currentUser.created_at).toLocaleDateString()}</small></p>
        <!-- Можно добавить кнопку редактирования профиля -->
    `;
}

async function loadMyCourses() { // Загрузка курсов, которые пользователь создал или на которые записан
    const container = document.getElementById('myCoursesContent'); // Новый ID
    if (!container) return;
    container.innerHTML = '<div class="loading my-3"></div>';
    
    try {
        // Пока просто получаем все курсы, нужна логика для "моих" курсов
        const coursesData = await api.getCourses(); 
        
        if (!coursesData || coursesData.length === 0) {
            container.innerHTML = '<div class="alert alert-light">У вас пока нет доступных курсов.</div>';
            return;
        }
        
        container.innerHTML = coursesData.map(course => `
            <div class="col-md-6 mb-3">
                <div class="card h-100 course-card">
                    <div class="card-body d-flex flex-column">
                        <h5 class="card-title">${course.title}</h5>
                        <p class="card-text small text-muted flex-grow-1">${course.description || 'Нет описания.'}</p>
                        <div class="mt-auto">
                            <span class="badge bg-primary me-1">${course.category}</span>
                            <span class="badge bg-secondary me-1">${course.level}</span>
                            <!-- <button class="btn btn-sm btn-primary mt-2" onclick="viewCourseDetails(${course.id})">Перейти к курсу</button> -->
                        </div>
                    </div>
                </div>
            </div>
        `).join('');
    } catch (error) {
        container.innerHTML = '<div class="alert alert-danger">Ошибка загрузки ваших курсов.</div>';
    }
}

async function loadUserProgress() {
    const container = document.getElementById('progressContent'); // Новый ID
    if (!container || !api.currentUser) return;
    container.innerHTML = '<div class="loading my-3"></div>';
    
    try {
        const progressDataList = await api.getUserProgress(); // API теперь возвращает список
        
        if (!progressDataList || progressDataList.length === 0) {
            container.innerHTML = '<div class="alert alert-light">Данные о прогрессе отсутствуют.</div>';
            return;
        }
        
        container.innerHTML = progressDataList.map(courseProgress => `
            <div class="progress-item mb-4 p-3 border rounded">
                <h6 class="mb-1">${courseProgress.course_title}</h6>
                <div class="progress mb-1" style="height: 20px;">
                    <div class="progress-bar bg-success" role="progressbar" 
                         style="width: ${courseProgress.completion_percentage.toFixed(0)}%;" 
                         aria-valuenow="${courseProgress.completion_percentage.toFixed(0)}" 
                         aria-valuemin="0" aria-valuemax="100">
                         ${courseProgress.completion_percentage.toFixed(0)}%
                    </div>
                </div>
                <div class="d-flex justify-content-between text-muted small">
                    <span>Материалов: ${courseProgress.completed_materials}/${courseProgress.total_materials}</span>
                    <span>Время: ${Math.round(courseProgress.total_time / 60)} мин</span>
                    ${courseProgress.avg_score !== null ? `<span>Ср. балл: ${courseProgress.avg_score.toFixed(1)}</span>` : ''}
                </div>
            </div>
        `).join('');
    } catch (error) {
        container.innerHTML = '<div class="alert alert-danger">Ошибка загрузки прогресса.</div>';
    }
}

// --- Utility functions ---
function showAlert(message, type = 'info', duration = 5000) {
    const alertContainerId = 'alert-container-fixed';
    let alertContainer = document.getElementById(alertContainerId);
    if (!alertContainer) {
        alertContainer = document.createElement('div');
        alertContainer.id = alertContainerId;
        alertContainer.style.cssText = 'position: fixed; top: 20px; right: 20px; z-index: 1056; min-width: 300px;';
        document.body.appendChild(alertContainer);
    }

    const alertEl = document.createElement('div');
    alertEl.className = `alert alert-${type} alert-dismissible fade show`;
    alertEl.setAttribute('role', 'alert');
    alertEl.innerHTML = `
        ${message}
        <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
    `;
    
    alertContainer.appendChild(alertEl);
    
    if (duration > 0) {
        setTimeout(() => {
            const bsAlert = bootstrap.Alert.getOrCreateInstance(alertEl);
            if (bsAlert) {
                bsAlert.close();
            } else { // Fallback if bootstrap instance not found
                alertEl.remove();
            }
        }, duration);
    }
}

async function handleCreateCourse(event) {
    event.preventDefault();
    const form = event.target;
    const createCourseModalEl = document.getElementById('createCourseModal');
    
    const courseData = {
        title: form.courseTitle.value,
        description: form.courseDescription.value,
        category: form.courseCategory.value,
        level: form.courseLevel.value,
        teacher_id: api.currentUser.id // Преподаватель - текущий пользователь
    };
    
    try {
        await api.createCourse(courseData);
        if (createCourseModalEl) bootstrap.Modal.getInstance(createCourseModalEl).hide();
        showAlert('Курс успешно создан!', 'success');
        if (window.location.pathname === '/dashboard') { // Обновить список курсов на дашборде
            loadMyCourses();
        }
        form.reset();
    } catch (error) {
        showAlert(error.message, 'danger');
    }
}

function showCreateCourseModal() {
    const createCourseModalEl = document.getElementById('createCourseModal');
    if (createCourseModalEl) {
        const modal = bootstrap.Modal.getOrCreateInstance(createCourseModalEl);
        modal.show();
    }
}

// Старая функция логирования активности, адаптируем
async function logOldActivity() { // Переименовал, чтобы не конфликтовать с api.logActivity
      const activityResultEl = document.getElementById('activity-result'); // ID из старого HTML
      if(!activityResultEl) return;

      activityResultEl.textContent = 'Логирование...';
      if (!api.token) { // Используем api.token
          activityResultEl.textContent = 'Ошибка: Требуется авторизация для логирования.';
          return;
      }

      const userIdInput = document.getElementById('activity-user-id'); // ID из старого HTML
      const materialIdInput = document.getElementById('activity-material-id');
      const actionInput = document.getElementById('activity-action');

      if (!userIdInput || !materialIdInput || !actionInput) return;

      const userId = userIdInput.value;
      const materialId = materialIdInput.value;
      const action = actionInput.value;

      if (!userId || !materialId || !action) {
          activityResultEl.textContent = 'Ошибка: User ID, Material ID и Action обязательны.';
          return;
      }

      const activityData = {
        user_id: parseInt(userId),
        material_id: parseInt(materialId),
        action: action,
        // timestamp будет установлен на сервере
      };

      try {
        const data = await api.logActivity(activityData); // Используем новый метод api
        activityResultEl.textContent = JSON.stringify(data, null, 2);
      } catch (error) {
        activityResultEl.textContent = 'Ошибка: ' + error.message;
      }
}

// Глобальные функции, которые могут вызываться из HTML напрямую (старый стиль)
window.registerUser = registerUser;
window.loginUser = loginUser;
window.getCourses = getCourses; // Для кнопки "Показать курсы" из старого HTML
window.getMaterials = getMaterials; // Для кнопки "Показать материалы" из старого HTML
window.performSearch = handleSearch; // Для кнопки "Искать" из старого HTML (форма не модальная)
window.logActivity = logOldActivity; // Для кнопки "Логировать" из старого HTML
window.showAuthModal = showAuthModal;
window.showSearchModal = showSearchModal;
window.showCreateCourseModal = showCreateCourseModal;
window.showLogin = showLogin;
window.showRegister = showRegister;

// Если у вас есть старый блок поиска без модального окна на какой-то странице,
// нужно убедиться, что у его кнопки `onclick` вызывает `handleSearchWithOldForm()`
// или адаптировать `handleSearch` для работы с обеими формами.
// Для простоты, предполагаем, что поиск теперь всегда через модальное окно.
// Если старая форма поиска остается, ее JavaScript нужно будет вызвать отдельно.
// В объединенном frontend.html старой формы поиска нет.