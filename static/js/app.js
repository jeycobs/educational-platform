/**
 * EduPlatform Frontend Application Script
 * 
 * Содержит всю клиентскую логику для образовательной платформы, включая:
 * - Взаимодействие с API (аутентификация, курсы, аналитика).
 * - Управление отображением данных на страницах.
 * - Обработку пользовательских событий (клики, отправка форм).
 *
 * Версия с централизованной инициализацией.
 */

// Отладочное сообщение, чтобы убедиться, что загружается свежая версия файла
console.log("ЗАГРУЖЕН ФАЙЛ APP.JS ВЕРСИЯ ОТ", new Date().toLocaleTimeString());


// ===================================================================
// 1. Класс для работы с API
// ===================================================================

class EduPlatformAPI {
    constructor() {
        this.baseURL = '';
        this.token = localStorage.getItem('access_token');
        this.currentUser = null;
    }

    /**
     * Инициализирует API: загружает данные текущего пользователя, если есть токен.
     */
    async init() {
        if (this.token) {
            await this.loadCurrentUser();
        }
        this.updateNavigation();
    }

    /**
     * Универсальный метод для отправки запросов к API.
     * @param {string} endpoint - Путь к API эндпоинту (например, '/courses').
     * @param {object} options - Опции для fetch() (method, body, headers и т.д.).
     * @returns {Promise<any>} - Данные ответа в формате JSON.
     */
    async request(endpoint, options = {}) {
        const url = this.baseURL + endpoint;
        const config = {
            headers: { 'Content-Type': 'application/json', ...options.headers },
            ...options
        };

        if (this.token) {
            config.headers['Authorization'] = `Bearer ${this.token}`;
        }

        try {
            const response = await fetch(url, config);

            // Обработка истекшей сессии
            if (response.status === 401 && endpoint !== '/token') {
                this.logout();
                showAlert('Сессия истекла. Пожалуйста, войдите снова.', 'warning');
                throw new Error('Unauthorized');
            }

            const responseData = await response.json().catch(() => ({ detail: `Request failed with status ${response.status}` }));

            if (!response.ok) {
                throw new Error(responseData.detail || `Request failed with status ${response.status}`);
            }

            return responseData;
        } catch (error) {
            console.error(`API request to ${endpoint} failed:`, error);
            throw error;
        }
    }

    // --- Методы аутентификации ---

    async login(email, password) {
        const formData = new FormData();
        formData.append('username', email);
        formData.append('password', password);

        const response = await fetch('/token', { method: 'POST', body: formData });
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
        if (window.location.pathname !== '/') {
            window.location.href = '/';
        }
    }

    async loadCurrentUser() {
        if (!this.token) return;
        try {
            this.currentUser = await this.request('/users/me');
        } catch (error) {
            console.error("Failed to load current user, token might be invalid.", error);
            this.token = null;
            localStorage.removeItem('access_token');
        }
    }

    // --- Методы работы с данными ---

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

    async search(params = {}) {
        const cleanedParams = Object.fromEntries(Object.entries(params).filter(([_, v]) => v != null && v !== ''));
        const queryParams = new URLSearchParams(cleanedParams);
        return await this.request(`/search?${queryParams.toString()}`);
    }

    async getUserProgress() {
        if (!this.currentUser) throw new Error("User not authenticated");
        return await this.request(`/analytics/user/${this.currentUser.id}/progress`);
    }

    async getMyActivities(limit = 20) {
        if (!this.currentUser) throw new Error("User not authenticated");
        return await this.request(`/users/me/activities?limit=${limit}`);
    }
    
    // --- Методы для UI ---

    updateNavigation() {
        const authNav = document.getElementById('authNav');
        const dashboardLink = document.querySelector('a[href="/dashboard"]');
        if (!authNav || !dashboardLink) return;

        if (this.currentUser) {
            authNav.innerHTML = `
                <li class="nav-item dropdown">
                    <a class="nav-link dropdown-toggle text-white" href="#" id="navbarDropdownUserMenu" role="button" data-bs-toggle="dropdown" aria-expanded="false">
                        <i class="bi bi-person-circle me-1"></i>${this.currentUser.name}
                    </a>
                    <ul class="dropdown-menu dropdown-menu-end" aria-labelledby="navbarDropdownUserMenu">
                        <li><a class="dropdown-item" href="/dashboard"><i class="bi bi-grid me-2"></i>Панель управления</a></li>
                        <li><hr class="dropdown-divider"></li>
                        <li><a class="dropdown-item" href="#" id="logoutButton"><i class="bi bi-box-arrow-right me-2"></i>Выйти</a></li>
                    </ul>
                </li>`;
            document.getElementById('logoutButton').addEventListener('click', (e) => { e.preventDefault(); this.logout(); });
            dashboardLink.style.display = 'block';
        } else {
            authNav.innerHTML = `
                <li class="nav-item">
                    <a class="nav-link text-white" href="#" onclick="showAuthModal()">
                        <i class="bi bi-box-arrow-in-right me-1"></i>Войти / Регистрация
                    </a>
                </li>`;
            dashboardLink.style.display = 'none';
        }
    }
}


// ===================================================================
// 2. Глобальный экземпляр API и Инициализация
// ===================================================================

const api = new EduPlatformAPI();

/**
 * Центральная точка входа для всей клиентской логики.
 * Запускается после загрузки HTML.
 */
document.addEventListener('DOMContentLoaded', () => {
    api.init().then(() => {
        // --- Глобальные обработчики (работают на всех страницах) ---
        document.getElementById('loginForm')?.addEventListener('submit', handleLogin);
        document.getElementById('registerForm')?.addEventListener('submit', handleRegister);
        document.getElementById('searchFormModal')?.addEventListener('submit', handleSearch);
        document.getElementById('createCourseForm')?.addEventListener('submit', handleCreateCourse);

        // --- Логика для конкретных страниц (определяется по наличию элементов) ---
        
        // Если на странице есть контейнер для курсов, это главная страница.
        if (document.getElementById('coursesContainer')) {
            loadFeaturedCourses();
        }
        
        // Если на странице есть навигация дашборда, это панель управления.
        if (document.getElementById('dashboardTabs')) {
            checkAuthAndLoadDashboard();
        }
    });
});


// ===================================================================
// 3. Функции-обработчики событий (Handlers)
// ===================================================================

async function handleLogin(event) {
    event.preventDefault();
    const form = event.target;
    const msgEl = document.getElementById('loginMsg');
    if (msgEl) msgEl.textContent = '';
    
    try {
        await api.login(form.username.value, form.password.value);
        bootstrap.Modal.getInstance(document.getElementById('authModal'))?.hide();
        showAlert('Успешный вход!', 'success');
        window.location.href = '/dashboard';
    } catch (error) {
        if (msgEl) msgEl.textContent = error.message;
    }
}

async function handleRegister(event) {
    event.preventDefault();
    const form = event.target;
    const msgEl = document.getElementById('registerMsg');
    if (msgEl) msgEl.textContent = '';
    
    const userData = { name: form.name.value, email: form.email.value, role: form.role.value, password: form.password.value };

    try {
        await api.register(userData);
        showAlert('Регистрация успешна! Теперь вы можете войти.', 'success');
        document.getElementById('loginEmail').value = userData.email;
        showLogin();
        form.reset();
    } catch (error) {
        if (msgEl) msgEl.textContent = error.message;
    }
}

async function handleSearch(event) {
    event.preventDefault();
    const form = event.target;
    const resultsEl = document.getElementById('searchResults');
    if (resultsEl) resultsEl.innerHTML = '<div class="loading"></div>';

    const params = { q: form.q.value, category: form.category.value, level: form.level.value, search_in_courses: form.search_in_courses.checked, search_in_materials: form.search_in_materials.checked, search_in_teachers: form.search_in_teachers.checked, teacher_name: form.teacher_name.value, material_type: form.material_type.value };

    try {
        const response = await api.search(params);
        displaySearchResults(response.results);
    } catch (error) {
        if (resultsEl) resultsEl.innerHTML = `<div class="alert alert-danger">${error.message}</div>`;
    }
}

async function handleCreateCourse(event) {
    event.preventDefault();
    const form = event.target;
    const courseData = { title: form.title.value, description: form.description.value, category: form.category.value, level: form.level.value };

    try {
        await api.createCourse(courseData);
        bootstrap.Modal.getInstance(document.getElementById('createCourseModal'))?.hide();
        showAlert('Курс успешно создан!', 'success');
        loadMyCourses();
    } catch (error) {
        showAlert(`Ошибка создания курса: ${error.message}`, 'danger');
    }
}


// ===================================================================
// 4. Функции отображения контента (UI/Display)
// ===================================================================

/**
 * Отображает карточки курсов в указанном контейнере.
 */
function displayCourses(courses, containerId) {
    const container = document.getElementById(containerId);
    if (!container) return;
    if (!courses || !courses.length) {
        container.innerHTML = '<p class="text-muted">Курсы не найдены.</p>';
        return;
    }
    container.innerHTML = courses.map(course => `
        <div class="col-md-6 col-lg-4 mb-4">
            <a href="/courses/${course.id}" class="text-decoration-none text-dark d-block h-100">
                <div class="card course-card h-100">
                    <div class="card-body d-flex flex-column">
                        <h5 class="card-title">${course.title}</h5>
                        <p class="card-text small text-muted flex-grow-1">${course.description || 'Описание отсутствует.'}</p>
                        <div class="mt-auto">
                            <span class="badge bg-primary me-1">${course.category}</span>
                            <span class="badge bg-secondary">${course.level}</span>
                        </div>
                    </div>
                </div>
            </a>
        </div>`).join('');
}

/**
 * Отображает результаты поиска.
 */
function displaySearchResults(results) {
    const container = document.getElementById('searchResults');
    if (!container) return;
    if (!results || !results.length) {
        container.innerHTML = '<div class="alert alert-info mt-3">Ничего не найдено.</div>';
        return;
    }
    const html = results.map(item => `
        <div class="card mb-3">
            <a href="/courses/${item.id}" class="stretched-link text-decoration-none text-dark">
                <div class="card-body">
                    <h6 class="card-title mb-1">${item.title} <span class="badge bg-secondary float-end">${item.type}</span></h6>
                    ${item.category ? `<span class="badge bg-info me-1">${item.category}</span>` : ''}
                    ${item.level ? `<span class="badge bg-light text-dark me-1">${item.level}</span>` : ''}
                    ${item.teacher_name ? `<small class="text-muted d-block mt-1">Преподаватель: ${item.teacher_name}</small>` : ''}
                    ${item.relevance_score ? `<small class="text-muted">Релевантность: ${item.relevance_score.toFixed(2)}</small>`: ''}
                </div>
            </a>
        </div>`).join('');
    container.innerHTML = `<h5 class="mt-3">Результаты поиска:</h5>${html}`;
}

/**
 * Загружает и отображает популярные курсы на главной странице.
 */
async function loadFeaturedCourses() {
    const container = document.getElementById('coursesContainer');
    if (!container) return;
    container.innerHTML = '<div class="loading"></div>';
    try {
        const coursesData = await api.getCourses({ limit: 6 });
        displayCourses(coursesData, 'coursesContainer');
    } catch (error) {
        container.innerHTML = '<div class="alert alert-danger">Не удалось загрузить курсы.</div>';
    }
}


// ===================================================================
// 5. Логика для страницы "Панель управления" (Dashboard)
// ===================================================================

async function checkAuthAndLoadDashboard() {
    if (!api.token) {
        showAlert('Для доступа к панели управления необходимо войти.', 'warning');
        setTimeout(() => { window.location.href = '/'; }, 2000);
        return;
    }
    if (!api.currentUser) {
        await api.loadCurrentUser();
    }
    if (!api.currentUser) {
        showAlert('Сессия недействительна. Пожалуйста, войдите снова.', 'danger');
        api.logout();
        return;
    }
    
    const createCourseBtn = document.getElementById('createCourseBtn');
    if (createCourseBtn) {
        createCourseBtn.style.display = (api.currentUser.role === 'teacher' || api.currentUser.role === 'admin') ? 'inline-block' : 'none';
    }
    
    loadUserProfile();
    loadMyCourses();

    document.querySelectorAll('#dashboardTabs .nav-link').forEach(tab => {
        tab.addEventListener('shown.bs.tab', event => {
            const tabId = event.target.getAttribute('href').substring(1);
            console.log(`[DEBUG] Tab shown, ID: ${tabId}`);
            if (tabId === 'progress-pane') loadUserProgress();
            else if (tabId === 'activity-pane') loadUserActivity();
        });
    });
}

async function loadUserProfile() {
    const container = document.getElementById('profileInfoContainer');
    if (!container || !api.currentUser) return;
    container.innerHTML = `
        <div class="text-center mb-3"><i class="bi bi-person-circle display-3 text-primary"></i></div>
        <h5 class="card-title text-center">${api.currentUser.name}</h5>
        <p class="card-text text-center text-muted mb-1">${api.currentUser.email}</p>
        <p class="text-center"><span class="badge bg-info">${api.currentUser.role}</span></p>
        <hr>
        <p class="card-text"><small class="text-muted">Зарегистрирован: ${new Date(api.currentUser.created_at).toLocaleDateString()}</small></p>`;
}

async function loadMyCourses() {
    const container = document.getElementById('myCoursesContent');
    if (!container) return;
    container.innerHTML = '<div class="loading my-3"></div>';
    try {
        const coursesData = await api.getCourses();
        displayCourses(coursesData, 'myCoursesContent');
    } catch (error) {
        container.innerHTML = '<div class="alert alert-danger">Ошибка загрузки курсов.</div>';
    }
}

async function loadUserProgress() {
    console.log("[DEBUG] loadUserProgress called");
    const container = document.getElementById('progressContent');
    if (!container || !api.currentUser) {
        console.error("Exit from loadUserProgress: no container or user is logged in.");
        return;
    }
    container.innerHTML = '<div class="loading my-3"></div>';
    try {
        console.log("[DEBUG] Fetching progress data...");
        const progressDataList = await api.getUserProgress();
        if (!progressDataList || !progressDataList.length) {
            container.innerHTML = '<div class="alert alert-light">Данные о прогрессе отсутствуют.</div>';
            return;
        }
        container.innerHTML = progressDataList.map(item => `
            <div class="progress-item mb-4 p-3 border rounded">
                <h6 class="mb-1">${item.course_title}</h6>
                <div class="progress mb-1" style="height: 20px;">
                    <div class="progress-bar bg-success" role="progressbar" style="width: ${item.completion_percentage.toFixed(0)}%;" aria-valuenow="${item.completion_percentage.toFixed(0)}">${item.completion_percentage.toFixed(0)}%</div>
                </div>
                <div class="d-flex justify-content-between text-muted small">
                    <span>Материалов: ${item.completed_materials}/${item.total_materials}</span>
                    <span>Время: ${Math.round(item.total_time / 60)} мин</span>
                    ${item.avg_score !== null ? `<span>Ср. балл: <strong>${item.avg_score.toFixed(1)}</strong></span>` : ''}
                </div>
            </div>`).join('');
    } catch (error) {
        console.error("Error in loadUserProgress:", error);
        container.innerHTML = `<div class="alert alert-danger">Ошибка загрузки прогресса.</div>`;
    }
}

async function loadUserActivity() {
    console.log("[DEBUG] loadUserActivity called");
    const container = document.getElementById('activityContent');
    if (!container || !api.currentUser) {
        console.error("Exit from loadUserActivity: no container or user is logged in.");
        return;
    }
    container.innerHTML = '<div class="loading my-3"></div>';
    try {
        console.log("[DEBUG] Fetching activity data...");
        const activities = await api.getMyActivities();
        if (!activities || !activities.length) {
            container.innerHTML = '<div class="alert alert-light">Нет недавней активности.</div>';
            return;
        }
        container.innerHTML = activities.map(act => `
            <div class="activity-item d-flex justify-content-between align-items-center py-2 border-bottom">
                <div class="d-flex align-items-center">
                    <i class="bi bi-${getActivityIcon(act.action, act.material_type)} me-3 fs-4 text-primary"></i>
                    <div>
                        <div class="fw-bold">${getActivityActionText(act.action)}</div>
                        <div class="text-muted small">"${act.material_title}"</div>
                    </div>
                </div>
                <small class="text-muted">${new Date(act.timestamp).toLocaleString()}</small>
            </div>`).join('');
    } catch (error) {
        console.error("Error in loadUserActivity:", error);
        container.innerHTML = `<div class="alert alert-danger">Ошибка загрузки активности.</div>`;
    }
}


// ===================================================================
// 6. Вспомогательные и глобальные функции
// ===================================================================

function getActivityIcon(action, materialType) {
    if (action === 'complete') return 'check-circle-fill text-success';
    if (action === 'submit_quiz') return 'patch-check-fill text-info';
    switch (materialType) {
        case 'video': return 'play-btn-fill';
        case 'text': return 'file-text';
        case 'quiz': return 'question-circle';
        default: return 'eye';
    }
}

function getActivityActionText(action) {
    switch (action) {
        case 'view': return 'Просмотрен материал';
        case 'start': return 'Начато изучение';
        case 'complete': return 'Завершен материал';
        case 'submit_quiz': return 'Сдан тест';
        default: return action;
    }
}

function showAlert(message, type = 'info', duration = 5000) {
    let container = document.getElementById('alert-container-fixed');
    if (!container) {
        container = document.createElement('div');
        container.id = 'alert-container-fixed';
        container.style.cssText = 'position: fixed; top: 20px; right: 20px; z-index: 1056; min-width: 300px;';
        document.body.appendChild(container);
    }
    const alertEl = document.createElement('div');
    alertEl.className = `alert alert-${type} alert-dismissible fade show`;
    alertEl.role = 'alert';
    alertEl.innerHTML = `${message}<button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>`;
    container.appendChild(alertEl);

    if (duration > 0) {
        setTimeout(() => {
            bootstrap.Alert.getOrCreateInstance(alertEl)?.close();
        }, duration);
    }
}

// --- Функции для вызова из HTML: делаем их глобальными ---

function updateAuthTabs(activeTab) {
    document.querySelectorAll('#authTabs .nav-link').forEach(tab => tab.classList.remove('active'));
    document.querySelector(`#authTabs .nav-link[data-auth-tab="${activeTab}"]`)?.classList.add('active');
}

window.showLogin = function() {
    document.getElementById('loginFormContent').style.display = 'block';
    document.getElementById('registerFormContent').style.display = 'none';
    document.getElementById('authModalTitle').textContent = 'Вход в систему';
    updateAuthTabs('login');
}

window.showRegister = function() {
    document.getElementById('loginFormContent').style.display = 'none';
    document.getElementById('registerFormContent').style.display = 'block';
    document.getElementById('authModalTitle').textContent = 'Регистрация';
    updateAuthTabs('register');
}

window.showAuthModal = function() {
    const modalEl = document.getElementById('authModal');
    if (modalEl) {
        bootstrap.Modal.getOrCreateInstance(modalEl).show();
        showLogin(); // По умолчанию показываем форму входа
    }
}

window.showSearchModal = function() {
    const modalEl = document.getElementById('searchModal');
    if (modalEl) bootstrap.Modal.getOrCreateInstance(modalEl).show();
}

window.showCreateCourseModal = function() {
    const modalEl = document.getElementById('createCourseModal');
    if (modalEl) bootstrap.Modal.getOrCreateInstance(modalEl).show();
}

window.performSearchWithCategory = function(category) {
    showSearchModal();
    // Небольшая задержка, чтобы модальное окно успело появиться
    setTimeout(() => {
        const categorySelect = document.getElementById('searchCategoryModal');
        const searchForm = document.getElementById('searchFormModal');
        if (categorySelect) categorySelect.value = category;
        if (searchForm) handleSearch({ preventDefault: () => {}, target: searchForm });
    }, 200);
}// --- Делаем функции доступными глобально для HTML ---
window.showAuthModal = showAuthModal;
window.showSearchModal = showSearchModal;
window.showCreateCourseModal = showCreateCourseModal;
window.showLogin = showLogin;
window.showRegister = showRegister;