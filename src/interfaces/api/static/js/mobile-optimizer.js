/**
 * Mobile Optimization Service
 * 
 * Enhances mobile experience with:
 * - Intelligent touch gesture handling
 * - Viewport optimization
 * - Performance optimizations for mobile devices
 * - Adaptive UI scaling
 * - Network-aware loading
 */

class MobileOptimizer {
    constructor() {
        this.isMobile = this.detectMobile();
        this.isTablet = this.detectTablet();
        this.connection = this.getConnectionType();
        this.viewportWidth = window.innerWidth;
        this.viewportHeight = window.innerHeight;
        this.orientation = this.getOrientation();
        
        // Performance tracking
        this.performanceMetrics = {
            renderStart: performance.now(),
            firstInteraction: null,
            layoutShifts: 0
        };
        
        this.init();
    }
    
    init() {
        if (this.isMobile || this.isTablet) {
            this.setupMobileOptimizations();
            this.setupTouchGestures();
            this.setupViewportHandling();
            this.setupPerformanceOptimizations();
            this.setupNetworkAdaptation();
        }
        
        this.setupResponsiveImages();
        this.setupLazyLoading();
        this.setupIntersectionObserver();
    }
    
    detectMobile() {
        const userAgent = navigator.userAgent.toLowerCase();
        const mobileRegex = /android|webos|iphone|ipad|ipod|blackberry|iemobile|opera mini/i;
        return mobileRegex.test(userAgent) || window.innerWidth < 768;
    }
    
    detectTablet() {
        const userAgent = navigator.userAgent.toLowerCase();
        const tabletRegex = /tablet|ipad|playbook|silk|(puffin(?!.*(IP|AP|WP)))|(android(?!.*mobile))/i;
        return tabletRegex.test(userAgent) || 
               (window.innerWidth >= 768 && window.innerWidth <= 1024);
    }
    
    getConnectionType() {
        if ('connection' in navigator) {
            const connection = navigator.connection || navigator.mozConnection || navigator.webkitConnection;
            return {
                effectiveType: connection.effectiveType || '4g',
                downlink: connection.downlink || 10,
                rtt: connection.rtt || 50,
                saveData: connection.saveData || false
            };
        }
        return { effectiveType: '4g', downlink: 10, rtt: 50, saveData: false };
    }
    
    getOrientation() {
        if (screen.orientation) {
            return screen.orientation.type;
        }
        return window.innerHeight > window.innerWidth ? 'portrait' : 'landscape';
    }
    
    setupMobileOptimizations() {
        // Add mobile-specific CSS classes
        document.documentElement.classList.add('mobile-device');
        
        if (this.isTablet) {
            document.documentElement.classList.add('tablet-device');
        }
        
        // Optimize viewport meta tag
        this.optimizeViewport();
        
        // Disable hover effects on touch devices
        this.disableHoverEffects();
        
        // Optimize scroll behavior
        this.optimizeScrolling();
        
        // Add safe area support for notched devices
        this.setupSafeArea();
    }
    
    optimizeViewport() {
        let viewportMeta = document.querySelector('meta[name="viewport"]');
        if (!viewportMeta) {
            viewportMeta = document.createElement('meta');
            viewportMeta.name = 'viewport';
            document.head.appendChild(viewportMeta);
        }
        
        // Prevent zoom on inputs while allowing user scaling
        viewportMeta.content = 'width=device-width, initial-scale=1.0, maximum-scale=5.0, user-scalable=yes, viewport-fit=cover';
    }
    
    disableHoverEffects() {
        // Add CSS to disable hover effects on touch devices
        const style = document.createElement('style');
        style.textContent = `
            @media (hover: none) {
                .widget-card:hover,
                .widget-control-btn:hover,
                .btn-primary:hover,
                .btn-secondary:hover {
                    transform: none !important;
                    background-color: initial !important;
                    border-color: initial !important;
                }
            }
        `;
        document.head.appendChild(style);
    }
    
    optimizeScrolling() {
        // Enable smooth scrolling with momentum
        document.documentElement.style.webkitOverflowScrolling = 'touch';
        
        // Prevent scroll bouncing on iOS
        document.addEventListener('touchmove', (e) => {
            if (e.target.closest('.widget-content')) {
                // Allow scrolling within widget content
                return;
            }
            
            // Check if we're at the top or bottom of the page
            const isAtTop = window.scrollY === 0;
            const isAtBottom = window.scrollY >= document.body.scrollHeight - window.innerHeight - 1;
            
            if ((isAtTop && e.touches[0].clientY > e.changedTouches[0].clientY) ||
                (isAtBottom && e.touches[0].clientY < e.changedTouches[0].clientY)) {
                e.preventDefault();
            }
        }, { passive: false });
    }
    
    setupSafeArea() {
        // Add CSS custom properties for safe areas
        const style = document.createElement('style');
        style.textContent = `
            :root {
                --safe-area-inset-top: env(safe-area-inset-top);
                --safe-area-inset-right: env(safe-area-inset-right);
                --safe-area-inset-bottom: env(safe-area-inset-bottom);
                --safe-area-inset-left: env(safe-area-inset-left);
            }
            
            .header {
                padding-top: calc(1rem + var(--safe-area-inset-top));
                padding-left: calc(2rem + var(--safe-area-inset-left));
                padding-right: calc(2rem + var(--safe-area-inset-right));
            }
            
            .dashboard-container {
                padding-left: calc(1.5rem + var(--safe-area-inset-left));
                padding-right: calc(1.5rem + var(--safe-area-inset-right));
                padding-bottom: calc(1.5rem + var(--safe-area-inset-bottom));
            }
        `;
        document.head.appendChild(style);
    }
    
    setupTouchGestures() {
        let touchStartX = 0;
        let touchStartY = 0;
        let touchStartTime = 0;
        let isLongPress = false;
        let longPressTimer;
        
        document.addEventListener('touchstart', (e) => {
            touchStartX = e.touches[0].clientX;
            touchStartY = e.touches[0].clientY;
            touchStartTime = Date.now();
            isLongPress = false;
            
            // Long press detection for context menus
            longPressTimer = setTimeout(() => {
                isLongPress = true;
                this.handleLongPress(e);
            }, 500);
            
            // Prevent double-tap zoom on buttons
            if (e.target.closest('button, .btn-primary, .btn-secondary')) {
                e.preventDefault();
            }
        }, { passive: false });
        
        document.addEventListener('touchmove', (e) => {
            const touchMoveX = e.touches[0].clientX;
            const touchMoveY = e.touches[0].clientY;
            const deltaX = Math.abs(touchMoveX - touchStartX);
            const deltaY = Math.abs(touchMoveY - touchStartY);
            
            // Cancel long press if moving too much
            if (deltaX > 10 || deltaY > 10) {
                clearTimeout(longPressTimer);
                isLongPress = false;
            }
            
            // Handle swipe gestures
            this.handleSwipeGesture(touchStartX, touchStartY, touchMoveX, touchMoveY);
        });
        
        document.addEventListener('touchend', (e) => {
            clearTimeout(longPressTimer);
            
            const touchEndTime = Date.now();
            const duration = touchEndTime - touchStartTime;
            
            if (!isLongPress && duration < 200) {
                this.handleTap(e);
            }
            
            // Track first interaction for performance
            if (this.performanceMetrics.firstInteraction === null) {
                this.performanceMetrics.firstInteraction = performance.now() - this.performanceMetrics.renderStart;
            }
        });
    }
    
    handleLongPress(e) {
        const widget = e.target.closest('.widget-card');
        if (widget) {
            // Haptic feedback
            if (navigator.vibrate) {
                navigator.vibrate(50);
            }
            
            // Show context menu
            this.showContextMenu(widget, e.touches[0].clientX, e.touches[0].clientY);
        }
    }
    
    handleSwipeGesture(startX, startY, currentX, currentY) {
        const deltaX = currentX - startX;
        const deltaY = currentY - startY;
        const threshold = 50;
        
        if (Math.abs(deltaX) > threshold || Math.abs(deltaY) > threshold) {
            if (Math.abs(deltaX) > Math.abs(deltaY)) {
                // Horizontal swipe
                if (deltaX > 0) {
                    this.handleSwipeRight();
                } else {
                    this.handleSwipeLeft();
                }
            } else {
                // Vertical swipe
                if (deltaY > 0) {
                    this.handleSwipeDown();
                } else {
                    this.handleSwipeUp();
                }
            }
        }
    }
    
    handleTap(e) {
        // Enhanced tap handling for better responsiveness
        const target = e.target;
        
        if (target.closest('.widget-refresh-btn')) {
            this.addTapFeedback(target);
        }
    }
    
    addTapFeedback(element) {
        element.style.transform = 'scale(0.95)';
        element.style.transition = 'transform 0.1s ease';
        
        setTimeout(() => {
            element.style.transform = '';
        }, 100);
    }
    
    handleSwipeRight() {
        // Navigate to previous layout preset
        const presets = document.querySelectorAll('.layout-preset-btn');
        const active = document.querySelector('.layout-preset-btn.active');
        if (active) {
            const currentIndex = Array.from(presets).indexOf(active);
            const prevIndex = (currentIndex - 1 + presets.length) % presets.length;
            presets[prevIndex].click();
        }
    }
    
    handleSwipeLeft() {
        // Navigate to next layout preset
        const presets = document.querySelectorAll('.layout-preset-btn');
        const active = document.querySelector('.layout-preset-btn.active');
        if (active) {
            const currentIndex = Array.from(presets).indexOf(active);
            const nextIndex = (currentIndex + 1) % presets.length;
            presets[nextIndex].click();
        }
    }
    
    handleSwipeDown() {
        // Refresh all widgets
        if (window.dashboard && window.dashboard.refreshAllActiveWidgets) {
            window.dashboard.refreshAllActiveWidgets();
            this.showSwipeFeedback('Refreshing all widgets...');
        }
    }
    
    handleSwipeUp() {
        // Show/hide dashboard toolbar
        const toolbar = document.querySelector('.dashboard-toolbar');
        if (toolbar) {
            toolbar.style.transform = toolbar.style.transform === 'translateY(-100%)' ? 
                'translateY(0)' : 'translateY(-100%)';
        }
    }
    
    showSwipeFeedback(message) {
        const feedback = document.createElement('div');
        feedback.className = 'swipe-feedback';
        feedback.textContent = message;
        feedback.style.cssText = `
            position: fixed;
            top: 50%;
            left: 50%;
            transform: translate(-50%, -50%);
            background: rgba(0, 0, 0, 0.8);
            color: white;
            padding: 1rem 2rem;
            border-radius: 8px;
            z-index: 2000;
            font-size: 0.875rem;
            backdrop-filter: blur(4px);
        `;
        
        document.body.appendChild(feedback);
        
        setTimeout(() => {
            feedback.remove();
        }, 2000);
    }
    
    showContextMenu(widget, x, y) {
        const widgetId = widget.dataset.widgetId;
        const contextMenu = document.createElement('div');
        contextMenu.className = 'widget-context-menu';
        contextMenu.style.cssText = `
            position: fixed;
            left: ${x}px;
            top: ${y}px;
            background: #1e293b;
            border: 1px solid #374151;
            border-radius: 8px;
            box-shadow: 0 10px 25px rgba(0, 0, 0, 0.5);
            z-index: 2000;
            padding: 0.5rem 0;
            min-width: 150px;
        `;
        
        const menuItems = [
            { label: 'Refresh', action: () => window.dashboard.refreshSpecificWidget(widgetId) },
            { label: 'Configure', action: () => window.widgetManager.openWidgetConfig(widgetId) },
            { label: 'Remove', action: () => window.widgetManager.removeWidget(widgetId) }
        ];
        
        menuItems.forEach(item => {
            const menuItem = document.createElement('div');
            menuItem.className = 'context-menu-item';
            menuItem.textContent = item.label;
            menuItem.style.cssText = `
                padding: 0.75rem 1rem;
                cursor: pointer;
                color: #e2e8f0;
                font-size: 0.875rem;
            `;
            
            menuItem.addEventListener('click', () => {
                item.action();
                contextMenu.remove();
            });
            
            menuItem.addEventListener('touchstart', (e) => {
                e.stopPropagation();
                menuItem.style.background = '#374151';
            });
            
            contextMenu.appendChild(menuItem);
        });
        
        // Close menu when clicking elsewhere
        const closeMenu = (e) => {
            if (!contextMenu.contains(e.target)) {
                contextMenu.remove();
                document.removeEventListener('click', closeMenu);
                document.removeEventListener('touchstart', closeMenu);
            }
        };
        
        setTimeout(() => {
            document.addEventListener('click', closeMenu);
            document.addEventListener('touchstart', closeMenu);
        }, 100);
        
        document.body.appendChild(contextMenu);
    }
    
    setupViewportHandling() {
        let resizeTimeout;
        
        window.addEventListener('resize', () => {
            clearTimeout(resizeTimeout);
            resizeTimeout = setTimeout(() => {
                this.handleViewportChange();
            }, 150);
        });
        
        // Handle orientation change
        window.addEventListener('orientationchange', () => {
            setTimeout(() => {
                this.handleOrientationChange();
            }, 100);
        });
    }
    
    handleViewportChange() {
        const newWidth = window.innerWidth;
        const newHeight = window.innerHeight;
        
        // Update viewport tracking
        this.viewportWidth = newWidth;
        this.viewportHeight = newHeight;
        
        // Adjust layout for new dimensions
        this.adjustLayoutForViewport();
        
        // Resize charts if they exist
        if (window.dashboard && window.dashboard.widgetManager) {
            document.querySelectorAll('.widget-chart-widget canvas').forEach(canvas => {
                if (canvas.chart && canvas.chart.resize) {
                    canvas.chart.resize();
                }
            });
        }
    }
    
    handleOrientationChange() {
        const newOrientation = this.getOrientation();
        
        if (newOrientation !== this.orientation) {
            this.orientation = newOrientation;
            
            // Add orientation class
            document.documentElement.classList.remove('portrait', 'landscape');
            document.documentElement.classList.add(
                newOrientation.includes('portrait') ? 'portrait' : 'landscape'
            );
            
            // Adjust widget sizes for orientation
            this.adjustWidgetsForOrientation();
        }
    }
    
    adjustLayoutForViewport() {
        const container = document.querySelector('.dashboard-container');
        if (!container) return;
        
        if (this.viewportWidth < 480) {
            container.classList.add('layout-compact');
        } else {
            container.classList.remove('layout-compact');
        }
    }
    
    adjustWidgetsForOrientation() {
        const isLandscape = this.orientation.includes('landscape');
        
        if (isLandscape && this.isMobile) {
            // In landscape mode on mobile, make widgets wider
            document.querySelectorAll('.widget-size-extra-large').forEach(widget => {
                widget.style.gridColumn = 'span 1';
            });
        }
    }
    
    setupPerformanceOptimizations() {
        // Reduce animation duration on slower devices
        if (this.connection.effectiveType === '2g' || this.connection.saveData) {
            this.enablePerformanceMode();
        }
        
        // Optimize rendering with passive listeners
        this.optimizeEventListeners();
        
        // Setup performance monitoring
        this.setupPerformanceMonitoring();
    }
    
    enablePerformanceMode() {
        document.documentElement.classList.add('performance-mode');
        
        // Reduce refresh intervals
        if (window.dashboard && window.dashboard.widgetManager) {
            window.dashboard.widgetManager.enablePerformanceMode();
        }
    }
    
    optimizeEventListeners() {
        // Use passive listeners where possible
        const passiveEvents = ['touchstart', 'touchmove', 'wheel', 'scroll'];
        
        passiveEvents.forEach(eventType => {
            document.addEventListener(eventType, () => {}, { passive: true });
        });
    }
    
    setupPerformanceMonitoring() {
        // Monitor layout shifts
        if ('LayoutShift' in window) {
            new PerformanceObserver((entryList) => {
                for (const entry of entryList.getEntries()) {
                    if (!entry.hadRecentInput) {
                        this.performanceMetrics.layoutShifts += entry.value;
                    }
                }
            }).observe({ entryTypes: ['layout-shift'] });
        }
        
        // Monitor long tasks
        if ('PerformanceObserver' in window) {
            new PerformanceObserver((entryList) => {
                for (const entry of entryList.getEntries()) {
                    if (entry.duration > 50) {
                        console.warn('Long task detected:', entry.duration + 'ms');
                    }
                }
            }).observe({ entryTypes: ['longtask'] });
        }
    }
    
    setupNetworkAdaptation() {
        // Adapt behavior based on connection
        if (this.connection.saveData || this.connection.effectiveType === '2g') {
            this.enableDataSaverMode();
        }
        
        // Listen for connection changes
        if ('connection' in navigator) {
            navigator.connection.addEventListener('change', () => {
                this.connection = this.getConnectionType();
                this.adaptToConnection();
            });
        }
    }
    
    enableDataSaverMode() {
        document.documentElement.classList.add('data-saver-mode');
        
        // Increase refresh intervals
        if (window.dashboard && window.dashboard.widgetManager) {
            window.dashboard.widgetManager.widgets.forEach(widget => {
                widget.refreshInterval = Math.max(widget.refreshInterval * 2, 10000);
            });
        }
        
        // Disable auto-refresh for non-critical widgets
        document.querySelectorAll('.widget-card:not([data-widget-id="system-health"])').forEach(widget => {
            const widgetId = widget.dataset.widgetId;
            if (window.dashboard && window.dashboard.widgetManager) {
                const widgetConfig = window.dashboard.widgetManager.widgets.get(widgetId);
                if (widgetConfig) {
                    widgetConfig.autoRefresh = false;
                }
            }
        });
    }
    
    adaptToConnection() {
        if (this.connection.effectiveType === '2g' || this.connection.saveData) {
            this.enableDataSaverMode();
        } else {
            document.documentElement.classList.remove('data-saver-mode');
        }
    }
    
    setupResponsiveImages() {
        // Lazy load images and adapt quality based on connection
        const images = document.querySelectorAll('img[data-src]');
        
        images.forEach(img => {
            if (this.connection.effectiveType === '2g') {
                img.dataset.src = img.dataset.src.replace(/quality=\d+/, 'quality=50');
            }
        });
    }
    
    setupLazyLoading() {
        // Setup lazy loading for widget content
        if (window.dashboard && window.dashboard.widgetManager) {
            window.dashboard.widgetManager.setupLazyLoading();
        }
    }
    
    setupIntersectionObserver() {
        // Pause updates for widgets not in viewport
        const observer = new IntersectionObserver((entries) => {
            entries.forEach(entry => {
                const widgetId = entry.target.dataset.widgetId;
                if (window.dashboard && window.dashboard.widgetManager) {
                    const widget = window.dashboard.widgetManager.widgets.get(widgetId);
                    if (widget) {
                        widget.isVisible = entry.isIntersecting;
                        
                        // Pause/resume refresh based on visibility
                        if (!entry.isIntersecting && widget.refreshTimer) {
                            clearInterval(widget.refreshTimer);
                            widget.refreshTimer = null;
                        } else if (entry.isIntersecting && widget.autoRefresh && !widget.refreshTimer) {
                            widget.refreshTimer = setInterval(() => {
                                window.dashboard.refreshSpecificWidget(widgetId);
                            }, widget.refreshInterval);
                        }
                    }
                }
            });
        }, { threshold: 0.1 });
        
        // Observe all widgets
        document.querySelectorAll('.widget-card').forEach(widget => {
            observer.observe(widget);
        });
    }
    
    // Public API methods
    getPerformanceMetrics() {
        return {
            ...this.performanceMetrics,
            currentMemoryUsage: performance.memory ? performance.memory.usedJSHeapSize : 0,
            connectionType: this.connection.effectiveType,
            viewport: { width: this.viewportWidth, height: this.viewportHeight },
            orientation: this.orientation
        };
    }
    
    optimizeForDevice() {
        if (this.isMobile) {
            this.enablePerformanceMode();
            this.setupNetworkAdaptation();
        }
    }
}

// Export for global access
window.MobileOptimizer = MobileOptimizer;