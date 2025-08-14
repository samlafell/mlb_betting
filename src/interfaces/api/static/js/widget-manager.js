/**
 * Widget Management System
 * 
 * Provides comprehensive widget customization capabilities including:
 * - Drag and drop widget reordering
 * - Widget visibility toggles  
 * - Custom widget configurations
 * - Layout persistence in localStorage
 * - Performance-optimized lazy loading
 * - Touch-optimized mobile interactions
 */

class WidgetManager {
    constructor(dashboardContainer) {
        this.container = dashboardContainer;
        this.widgets = new Map();
        this.layouts = new Map();
        this.preferences = this.loadPreferences();
        this.draggedWidget = null;
        this.touchStartY = 0;
        this.touchStartX = 0;
        this.isTouch = 'ontouchstart' in window;
        
        this.initializeWidgets();
        this.setupEventListeners();
        this.setupDragAndDrop();
        this.setupTouchOptimizations();
    }
    
    initializeWidgets() {
        // Default widget configurations
        this.defaultWidgets = {
            'system-health': {
                id: 'system-health',
                title: '📊 System Health',
                type: 'metric-card',
                refreshInterval: 5000,
                priority: 1,
                resizable: false,
                removable: false,
                configurable: true,
                defaultSize: 'medium',
                categories: ['monitoring', 'system']
            },
            'active-pipelines': {
                id: 'active-pipelines', 
                title: '🔄 Active Pipelines',
                type: 'list-widget',
                refreshInterval: 3000,
                priority: 2,
                resizable: true,
                removable: true,
                configurable: true,
                defaultSize: 'large',
                categories: ['pipeline', 'monitoring']
            },
            'performance-metrics': {
                id: 'performance-metrics',
                title: '📈 Performance Metrics', 
                type: 'metric-grid',
                refreshInterval: 10000,
                priority: 3,
                resizable: true,
                removable: true,
                configurable: true,
                defaultSize: 'medium',
                categories: ['analytics', 'performance']
            },
            'recent-activity': {
                id: 'recent-activity',
                title: '📝 Recent Activity',
                type: 'timeline-widget',
                refreshInterval: 15000,
                priority: 4,
                resizable: true, 
                removable: true,
                configurable: true,
                defaultSize: 'large',
                categories: ['activity', 'monitoring']
            },
            'opportunities': {
                id: 'opportunities',
                title: '🎯 Betting Opportunities',
                type: 'opportunity-widget',
                refreshInterval: 5000,
                priority: 5,
                resizable: true,
                removable: true,
                configurable: true,
                defaultSize: 'extra-large',
                categories: ['betting', 'opportunities']
            },
            'line-movement': {
                id: 'line-movement',
                title: '📈 Live Line Movement',
                type: 'chart-widget',
                refreshInterval: 2000,
                priority: 6,
                resizable: true,
                removable: true,
                configurable: true,
                defaultSize: 'extra-large',
                categories: ['betting', 'analytics']
            },
            'alerts': {
                id: 'alerts',
                title: '🚨 System Alerts',
                type: 'alert-widget',
                refreshInterval: 1000,
                priority: 7,
                resizable: false,
                removable: true,
                configurable: true,
                defaultSize: 'small',
                categories: ['alerts', 'system']
            },
            'quick-stats': {
                id: 'quick-stats',
                title: '⚡ Quick Stats',
                type: 'stat-widget',
                refreshInterval: 30000,
                priority: 8,
                resizable: false,
                removable: true,
                configurable: false,
                defaultSize: 'small',
                categories: ['stats', 'overview']
            }
        };
        
        // Load user's widget preferences
        this.loadUserWidgets();
    }
    
    loadUserWidgets() {
        const savedWidgets = this.preferences.widgets || {};
        const activeWidgets = this.preferences.activeWidgets || Object.keys(this.defaultWidgets);
        
        // Initialize active widgets with user preferences
        activeWidgets.forEach(widgetId => {
            if (this.defaultWidgets[widgetId]) {
                const config = {
                    ...this.defaultWidgets[widgetId],
                    ...savedWidgets[widgetId]
                };
                this.widgets.set(widgetId, config);
            }
        });
        
        // Apply saved layout
        this.applyLayout();
    }
    
    setupEventListeners() {
        // Widget configuration button listeners
        document.addEventListener('click', (e) => {
            if (e.target.classList.contains('widget-config-btn')) {
                this.openWidgetConfig(e.target.dataset.widgetId);
            }
            
            if (e.target.classList.contains('widget-remove-btn')) {
                this.removeWidget(e.target.dataset.widgetId);
            }
            
            if (e.target.classList.contains('widget-refresh-btn')) {
                this.refreshWidget(e.target.dataset.widgetId);
            }
        });
        
        // Layout preset buttons
        document.addEventListener('click', (e) => {
            if (e.target.classList.contains('layout-preset-btn')) {
                this.applyLayoutPreset(e.target.dataset.preset);
            }
        });
        
        // Add widget modal
        document.addEventListener('click', (e) => {
            if (e.target.id === 'add-widget-btn') {
                this.showAddWidgetModal();
            }
            
            if (e.target.id === 'dashboard-settings-btn') {
                this.showDashboardSettings();
            }
        });
    }
    
    setupDragAndDrop() {
        if (!this.container) return;
        
        // Enable HTML5 drag and drop for desktop
        this.container.addEventListener('dragstart', (e) => {
            if (e.target.closest('.widget-card')) {
                this.draggedWidget = e.target.closest('.widget-card');
                this.draggedWidget.style.opacity = '0.5';
                e.dataTransfer.effectAllowed = 'move';
                e.dataTransfer.setData('text/html', this.draggedWidget.outerHTML);
            }
        });
        
        this.container.addEventListener('dragover', (e) => {
            e.preventDefault();
            e.dataTransfer.dropEffect = 'move';
            
            const afterElement = this.getDragAfterElement(e.clientY);
            if (afterElement == null) {
                this.container.appendChild(this.draggedWidget);
            } else {
                this.container.insertBefore(this.draggedWidget, afterElement);
            }
        });
        
        this.container.addEventListener('dragend', (e) => {
            if (this.draggedWidget) {
                this.draggedWidget.style.opacity = '';
                this.saveLayout();
                this.draggedWidget = null;
            }
        });
        
        this.container.addEventListener('drop', (e) => {
            e.preventDefault();
        });
        
        // Make all widgets draggable
        this.updateDraggableWidgets();
    }
    
    setupTouchOptimizations() {
        if (!this.isTouch) return;
        
        let touchTimeout;
        
        this.container.addEventListener('touchstart', (e) => {
            const widget = e.target.closest('.widget-card');
            if (!widget) return;
            
            this.touchStartY = e.touches[0].clientY;
            this.touchStartX = e.touches[0].clientX;
            
            // Long press to start drag on mobile
            touchTimeout = setTimeout(() => {
                this.startTouchDrag(widget, e.touches[0]);
            }, 500);
        });
        
        this.container.addEventListener('touchmove', (e) => {
            if (touchTimeout) {
                clearTimeout(touchTimeout);
                touchTimeout = null;
            }
            
            if (this.draggedWidget) {
                e.preventDefault();
                this.handleTouchDrag(e.touches[0]);
            }
        });
        
        this.container.addEventListener('touchend', (e) => {
            if (touchTimeout) {
                clearTimeout(touchTimeout);
                touchTimeout = null;
            }
            
            if (this.draggedWidget) {
                this.endTouchDrag();
            }
        });
    }
    
    startTouchDrag(widget, touch) {
        this.draggedWidget = widget;
        widget.classList.add('dragging-touch');
        
        // Create visual feedback
        navigator.vibrate && navigator.vibrate(50);
    }
    
    handleTouchDrag(touch) {
        if (!this.draggedWidget) return;
        
        // Move widget visually
        const rect = this.draggedWidget.getBoundingClientRect();
        const offsetY = touch.clientY - this.touchStartY;
        
        this.draggedWidget.style.transform = `translateY(${offsetY}px)`;
        
        // Find drop target
        const afterElement = this.getDragAfterElement(touch.clientY);
        if (afterElement && afterElement !== this.draggedWidget) {
            this.container.insertBefore(this.draggedWidget, afterElement);
        }
    }
    
    endTouchDrag() {
        if (!this.draggedWidget) return;
        
        this.draggedWidget.classList.remove('dragging-touch');
        this.draggedWidget.style.transform = '';
        this.saveLayout();
        this.draggedWidget = null;
    }
    
    getDragAfterElement(y) {
        const draggableElements = [...this.container.querySelectorAll('.widget-card:not(.dragging-touch)')];
        
        return draggableElements.reduce((closest, child) => {
            const box = child.getBoundingClientRect();
            const offset = y - box.top - box.height / 2;
            
            if (offset < 0 && offset > closest.offset) {
                return { offset: offset, element: child };
            } else {
                return closest;
            }
        }, { offset: Number.NEGATIVE_INFINITY }).element;
    }
    
    updateDraggableWidgets() {
        this.container.querySelectorAll('.widget-card').forEach(widget => {
            widget.draggable = true;
        });
    }
    
    addWidget(widgetId, config = null) {
        if (!this.defaultWidgets[widgetId]) {
            console.error(`Widget ${widgetId} not found in default widgets`);
            return;
        }
        
        const widgetConfig = config || this.defaultWidgets[widgetId];
        this.widgets.set(widgetId, widgetConfig);
        
        this.renderWidget(widgetId);
        this.savePreferences();
        
        // Trigger refresh of new widget
        setTimeout(() => this.refreshWidget(widgetId), 100);
    }
    
    removeWidget(widgetId) {
        if (!this.widgets.has(widgetId)) return;
        
        const widget = this.widgets.get(widgetId);
        if (!widget.removable) {
            this.showNotification('This widget cannot be removed', 'warning');
            return;
        }
        
        // Remove from DOM
        const widgetElement = document.getElementById(`widget-${widgetId}`);
        if (widgetElement) {
            widgetElement.remove();
        }
        
        // Remove from widgets map
        this.widgets.delete(widgetId);
        this.savePreferences();
        
        this.showNotification(`${widget.title} widget removed`, 'success');
    }
    
    refreshWidget(widgetId) {
        const widget = this.widgets.get(widgetId);
        if (!widget) return;
        
        const widgetElement = document.getElementById(`widget-${widgetId}`);
        if (!widgetElement) return;
        
        // Add refresh animation
        const contentElement = widgetElement.querySelector('.widget-content');
        if (contentElement) {
            contentElement.classList.add('refreshing');
            
            // Trigger widget-specific refresh logic
            this.triggerWidgetRefresh(widgetId);
            
            setTimeout(() => {
                contentElement.classList.remove('refreshing');
            }, 1000);
        }
    }
    
    triggerWidgetRefresh(widgetId) {
        // Dispatch custom event for widget refresh
        const event = new CustomEvent('widgetRefresh', {
            detail: { widgetId, timestamp: new Date() }
        });
        document.dispatchEvent(event);
    }
    
    renderWidget(widgetId) {
        const widget = this.widgets.get(widgetId);
        if (!widget) return;
        
        const widgetElement = this.createWidgetElement(widgetId, widget);
        
        // Insert widget in correct position based on priority
        const insertPosition = this.findInsertPosition(widget.priority);
        if (insertPosition) {
            this.container.insertBefore(widgetElement, insertPosition);
        } else {
            this.container.appendChild(widgetElement);
        }
        
        this.updateDraggableWidgets();
    }
    
    createWidgetElement(widgetId, widget) {
        const widgetElement = document.createElement('div');
        widgetElement.id = `widget-${widgetId}`;
        widgetElement.className = `widget-card widget-${widget.type} widget-size-${widget.size || widget.defaultSize}`;
        widgetElement.dataset.widgetId = widgetId;
        widgetElement.dataset.priority = widget.priority;
        
        widgetElement.innerHTML = `
            <div class="widget-header">
                <div class="widget-title">
                    <span class="widget-icon">${widget.title.split(' ')[0]}</span>
                    <span class="widget-name">${widget.title.substring(widget.title.indexOf(' ') + 1)}</span>
                </div>
                <div class="widget-controls">
                    <button class="widget-control-btn widget-refresh-btn" data-widget-id="${widgetId}" title="Refresh">
                        🔄
                    </button>
                    ${widget.configurable ? `
                        <button class="widget-control-btn widget-config-btn" data-widget-id="${widgetId}" title="Configure">
                            ⚙️
                        </button>
                    ` : ''}
                    ${widget.removable ? `
                        <button class="widget-control-btn widget-remove-btn" data-widget-id="${widgetId}" title="Remove">
                            ✕
                        </button>
                    ` : ''}
                    <div class="widget-drag-handle" title="Drag to reorder">⋮⋮</div>
                </div>
            </div>
            <div class="widget-content" id="${widgetId}">
                <div class="loading-placeholder">Loading ${widget.title}...</div>
            </div>
        `;
        
        return widgetElement;
    }
    
    findInsertPosition(priority) {
        const widgets = [...this.container.querySelectorAll('.widget-card')];
        return widgets.find(w => parseInt(w.dataset.priority) > priority);
    }
    
    openWidgetConfig(widgetId) {
        const widget = this.widgets.get(widgetId);
        if (!widget) return;
        
        const modal = this.createConfigModal(widgetId, widget);
        document.body.appendChild(modal);
        
        // Show modal with animation
        requestAnimationFrame(() => {
            modal.classList.add('show');
        });
    }
    
    createConfigModal(widgetId, widget) {
        const modal = document.createElement('div');
        modal.className = 'widget-config-modal';
        modal.id = `config-modal-${widgetId}`;
        
        modal.innerHTML = `
            <div class="modal-overlay" onclick="this.closest('.widget-config-modal').remove()"></div>
            <div class="modal-content">
                <div class="modal-header">
                    <h3>Configure ${widget.title}</h3>
                    <button class="modal-close" onclick="this.closest('.widget-config-modal').remove()">✕</button>
                </div>
                <div class="modal-body">
                    <div class="config-section">
                        <label>Widget Size</label>
                        <select id="widget-size-${widgetId}" class="config-select">
                            <option value="small" ${widget.size === 'small' ? 'selected' : ''}>Small</option>
                            <option value="medium" ${widget.size === 'medium' ? 'selected' : ''}>Medium</option>
                            <option value="large" ${widget.size === 'large' ? 'selected' : ''}>Large</option>
                            <option value="extra-large" ${widget.size === 'extra-large' ? 'selected' : ''}>Extra Large</option>
                        </select>
                    </div>
                    
                    <div class="config-section">
                        <label>Refresh Interval (seconds)</label>
                        <input type="number" id="refresh-interval-${widgetId}" 
                               value="${widget.refreshInterval / 1000}" min="1" max="300" 
                               class="config-input">
                    </div>
                    
                    <div class="config-section">
                        <label>
                            <input type="checkbox" id="auto-refresh-${widgetId}" 
                                   ${widget.autoRefresh !== false ? 'checked' : ''}>
                            Auto Refresh
                        </label>
                    </div>
                    
                    ${this.getWidgetSpecificConfig(widgetId, widget)}
                </div>
                <div class="modal-footer">
                    <button class="btn-secondary" onclick="this.closest('.widget-config-modal').remove()">
                        Cancel
                    </button>
                    <button class="btn-primary" onclick="window.widgetManager.saveWidgetConfig('${widgetId}')">
                        Save Changes
                    </button>
                </div>
            </div>
        `;
        
        return modal;
    }
    
    getWidgetSpecificConfig(widgetId, widget) {
        switch (widget.type) {
            case 'chart-widget':
                return `
                    <div class="config-section">
                        <label>Chart Type</label>
                        <select id="chart-type-${widgetId}" class="config-select">
                            <option value="line">Line Chart</option>
                            <option value="candlestick">Candlestick Chart</option>
                            <option value="area">Area Chart</option>
                        </select>
                    </div>
                    <div class="config-section">
                        <label>Time Range</label>
                        <select id="time-range-${widgetId}" class="config-select">
                            <option value="1h">1 Hour</option>
                            <option value="6h">6 Hours</option>
                            <option value="24h">24 Hours</option>
                            <option value="7d">7 Days</option>
                        </select>
                    </div>
                `;
            case 'metric-grid':
                return `
                    <div class="config-section">
                        <label>Metrics to Display</label>
                        <div class="checkbox-group">
                            <label><input type="checkbox" checked> CPU Usage</label>
                            <label><input type="checkbox" checked> Memory Usage</label>
                            <label><input type="checkbox" checked> Disk Usage</label>
                            <label><input type="checkbox" checked> Network I/O</label>
                        </div>
                    </div>
                `;
            case 'list-widget':
                return `
                    <div class="config-section">
                        <label>Items to Show</label>
                        <input type="number" id="list-items-${widgetId}" 
                               value="${widget.maxItems || 10}" min="1" max="50" 
                               class="config-input">
                    </div>
                `;
            default:
                return '';
        }
    }
    
    saveWidgetConfig(widgetId) {
        const widget = this.widgets.get(widgetId);
        if (!widget) return;
        
        // Get config values from modal
        const size = document.getElementById(`widget-size-${widgetId}`)?.value;
        const refreshInterval = parseInt(document.getElementById(`refresh-interval-${widgetId}`)?.value) * 1000;
        const autoRefresh = document.getElementById(`auto-refresh-${widgetId}`)?.checked;
        
        // Update widget config
        widget.size = size;
        widget.refreshInterval = refreshInterval;
        widget.autoRefresh = autoRefresh;
        
        // Apply visual changes
        const widgetElement = document.getElementById(`widget-${widgetId}`);
        if (widgetElement && size) {
            widgetElement.className = widgetElement.className.replace(/widget-size-\w+/, `widget-size-${size}`);
        }
        
        // Save preferences
        this.savePreferences();
        
        // Close modal
        document.getElementById(`config-modal-${widgetId}`)?.remove();
        
        this.showNotification('Widget configuration saved', 'success');
    }
    
    showAddWidgetModal() {
        const availableWidgets = Object.entries(this.defaultWidgets)
            .filter(([id]) => !this.widgets.has(id));
        
        if (availableWidgets.length === 0) {
            this.showNotification('All widgets are already added', 'info');
            return;
        }
        
        const modal = document.createElement('div');
        modal.className = 'add-widget-modal';
        modal.innerHTML = `
            <div class="modal-overlay" onclick="this.remove()"></div>
            <div class="modal-content">
                <div class="modal-header">
                    <h3>Add Widget</h3>
                    <button class="modal-close" onclick="this.closest('.add-widget-modal').remove()">✕</button>
                </div>
                <div class="modal-body">
                    <div class="widget-grid">
                        ${availableWidgets.map(([id, widget]) => `
                            <div class="widget-preview" data-widget-id="${id}">
                                <div class="widget-preview-header">
                                    <span class="widget-preview-icon">${widget.title.split(' ')[0]}</span>
                                    <span class="widget-preview-title">${widget.title.substring(widget.title.indexOf(' ') + 1)}</span>
                                </div>
                                <div class="widget-preview-description">
                                    Type: ${widget.type}<br>
                                    Categories: ${widget.categories.join(', ')}<br>
                                    Refresh: ${widget.refreshInterval / 1000}s
                                </div>
                                <button class="btn-primary add-widget-action" data-widget-id="${id}">
                                    Add Widget
                                </button>
                            </div>
                        `).join('')}
                    </div>
                </div>
            </div>
        `;
        
        // Add event listeners for add buttons
        modal.addEventListener('click', (e) => {
            if (e.target.classList.contains('add-widget-action')) {
                const widgetId = e.target.dataset.widgetId;
                this.addWidget(widgetId);
                modal.remove();
            }
        });
        
        document.body.appendChild(modal);
        requestAnimationFrame(() => modal.classList.add('show'));
    }
    
    applyLayoutPreset(presetName) {
        const presets = {
            'default': {
                widgets: ['system-health', 'active-pipelines', 'performance-metrics', 'recent-activity'],
                layout: 'grid'
            },
            'monitoring': {
                widgets: ['system-health', 'active-pipelines', 'alerts', 'performance-metrics', 'recent-activity'],
                layout: 'grid'
            },
            'betting': {
                widgets: ['opportunities', 'line-movement', 'quick-stats', 'system-health'],
                layout: 'grid'
            },
            'analytics': {
                widgets: ['performance-metrics', 'line-movement', 'opportunities', 'quick-stats'],
                layout: 'grid'
            },
            'minimal': {
                widgets: ['system-health', 'quick-stats'],
                layout: 'list'
            }
        };
        
        const preset = presets[presetName];
        if (!preset) return;
        
        // Clear current widgets
        this.container.innerHTML = '';
        this.widgets.clear();
        
        // Add preset widgets
        preset.widgets.forEach(widgetId => {
            this.addWidget(widgetId);
        });
        
        this.showNotification(`Applied ${presetName} layout preset`, 'success');
    }
    
    applyLayout() {
        const layout = this.preferences.layout;
        if (!layout) return;
        
        // Apply widget order
        if (layout.order) {
            layout.order.forEach((widgetId, index) => {
                const widget = document.getElementById(`widget-${widgetId}`);
                if (widget) {
                    this.container.appendChild(widget);
                }
            });
        }
    }
    
    saveLayout() {
        const order = [...this.container.querySelectorAll('.widget-card')]
            .map(w => w.dataset.widgetId);
        
        this.preferences.layout = { order };
        this.savePreferences();
    }
    
    loadPreferences() {
        const stored = localStorage.getItem('dashboard-preferences');
        return stored ? JSON.parse(stored) : {
            widgets: {},
            activeWidgets: null,
            layout: null,
            theme: 'dark'
        };
    }
    
    savePreferences() {
        // Update preferences object
        this.preferences.activeWidgets = Array.from(this.widgets.keys());
        this.preferences.widgets = Object.fromEntries(
            Array.from(this.widgets.entries()).map(([id, config]) => [
                id, 
                {
                    size: config.size,
                    refreshInterval: config.refreshInterval,
                    autoRefresh: config.autoRefresh,
                    customConfig: config.customConfig || {}
                }
            ])
        );
        
        localStorage.setItem('dashboard-preferences', JSON.stringify(this.preferences));
    }
    
    showNotification(message, type = 'info') {
        const notification = document.createElement('div');
        notification.className = `notification notification-${type}`;
        notification.innerHTML = `
            <span class="notification-message">${message}</span>
            <button class="notification-close" onclick="this.parentElement.remove()">✕</button>
        `;
        
        const container = document.getElementById('notification-container') || (() => {
            const container = document.createElement('div');
            container.id = 'notification-container';
            container.className = 'notification-container';
            document.body.appendChild(container);
            return container;
        })();
        
        container.appendChild(notification);
        
        // Auto-remove after 5 seconds
        setTimeout(() => {
            if (notification.parentElement) {
                notification.remove();
            }
        }, 5000);
    }
    
    // Performance optimization methods
    enablePerformanceMode() {
        // Reduce refresh intervals for performance
        this.widgets.forEach(widget => {
            widget.refreshInterval = Math.max(widget.refreshInterval * 2, 5000);
        });
        
        // Disable animations
        document.body.classList.add('performance-mode');
    }
    
    setupLazyLoading() {
        // Implement intersection observer for lazy loading widget content
        const observer = new IntersectionObserver((entries) => {
            entries.forEach(entry => {
                if (entry.isIntersecting) {
                    const widgetId = entry.target.dataset.widgetId;
                    this.loadWidgetContent(widgetId);
                    observer.unobserve(entry.target);
                }
            });
        }, { threshold: 0.1 });
        
        // Observe all widgets
        this.container.querySelectorAll('.widget-card').forEach(widget => {
            observer.observe(widget);
        });
    }
    
    loadWidgetContent(widgetId) {
        // Trigger content loading for widget
        this.triggerWidgetRefresh(widgetId);
    }
    
    // Export/Import functionality
    exportConfiguration() {
        const config = {
            preferences: this.preferences,
            version: '1.0',
            timestamp: new Date().toISOString()
        };
        
        const blob = new Blob([JSON.stringify(config, null, 2)], { type: 'application/json' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `dashboard-config-${new Date().toISOString().split('T')[0]}.json`;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(url);
    }
    
    importConfiguration(file) {
        const reader = new FileReader();
        reader.onload = (e) => {
            try {
                const config = JSON.parse(e.target.result);
                if (config.preferences) {
                    this.preferences = config.preferences;
                    this.savePreferences();
                    location.reload(); // Reload to apply new configuration
                }
            } catch (error) {
                this.showNotification('Invalid configuration file', 'error');
            }
        };
        reader.readAsText(file);
    }
}

// Export for global access
window.WidgetManager = WidgetManager;