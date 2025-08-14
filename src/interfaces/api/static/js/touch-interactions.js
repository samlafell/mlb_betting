/**
 * Touch Interaction Patterns for Mobile Design System
 * 
 * Comprehensive touch gesture handling system providing:
 * - Multi-touch gesture recognition
 * - Adaptive touch targets
 * - Haptic feedback integration
 * - Touch accessibility features
 * - Performance-optimized event handling
 * 
 * Created for Issue #56: Mobile Design System
 */

class TouchInteractionManager {
    constructor() {
        this.touchState = {
            isTouch: false,
            touches: new Map(),
            gestures: {
                tap: null,
                longPress: null,
                swipe: null,
                pinch: null,
                pan: null
            },
            settings: {
                tapTimeout: 300,
                longPressTimeout: 500,
                swipeThreshold: 50,
                swipeVelocityThreshold: 0.3,
                pinchThreshold: 10,
                panThreshold: 10,
                doubleTapTimeout: 300
            }
        };
        
        this.touchTargets = new Map();
        this.eventQueue = [];
        this.rafId = null;
        
        this.init();
    }
    
    init() {
        this.detectTouchCapability();
        this.setupEventListeners();
        this.enhanceTouchTargets();
        this.initializeHapticFeedback();
        this.setupAccessibilityFeatures();
    }
    
    detectTouchCapability() {
        this.touchState.isTouch = 'ontouchstart' in window || 
                                  navigator.maxTouchPoints > 0 || 
                                  navigator.msMaxTouchPoints > 0;
        
        if (this.touchState.isTouch) {
            document.documentElement.classList.add('touch-device');
        }
        
        // Detect device capabilities
        this.deviceCapabilities = {
            hasHapticFeedback: 'vibrate' in navigator,
            hasPointerEvents: 'PointerEvent' in window,
            hasTouch: this.touchState.isTouch,
            maxTouchPoints: navigator.maxTouchPoints || 1,
            devicePixelRatio: window.devicePixelRatio || 1
        };
    }
    
    setupEventListeners() {
        // Use pointer events if available, otherwise fall back to touch events
        if (this.deviceCapabilities.hasPointerEvents) {
            this.setupPointerEvents();
        } else if (this.deviceCapabilities.hasTouch) {
            this.setupTouchEvents();
        }
        
        // Mouse events for desktop fallback
        this.setupMouseEvents();
        
        // Prevent context menu on long press for better UX
        document.addEventListener('contextmenu', this.handleContextMenu.bind(this), { passive: false });
    }
    
    setupPointerEvents() {
        const events = ['pointerdown', 'pointermove', 'pointerup', 'pointercancel'];
        
        events.forEach(event => {
            document.addEventListener(event, (e) => {
                this.queueEvent(event, e);
            }, { passive: false });
        });
    }
    
    setupTouchEvents() {
        const events = ['touchstart', 'touchmove', 'touchend', 'touchcancel'];
        
        events.forEach(event => {
            document.addEventListener(event, (e) => {
                this.queueEvent(event, e);
            }, { passive: false });
        });
    }
    
    setupMouseEvents() {
        const events = ['mousedown', 'mousemove', 'mouseup'];
        
        events.forEach(event => {
            document.addEventListener(event, (e) => {
                if (!this.touchState.isTouch) {
                    this.queueEvent(event, e);
                }
            }, { passive: false });
        });
    }
    
    queueEvent(type, event) {
        this.eventQueue.push({ type, event, timestamp: performance.now() });
        
        if (!this.rafId) {
            this.rafId = requestAnimationFrame(() => {
                this.processEventQueue();
                this.rafId = null;
            });
        }
    }
    
    processEventQueue() {
        while (this.eventQueue.length > 0) {
            const { type, event } = this.eventQueue.shift();
            this.processEvent(type, event);
        }
    }
    
    processEvent(type, event) {
        switch (type) {
            case 'touchstart':
            case 'pointerdown':
            case 'mousedown':
                this.handleStart(event);
                break;
            case 'touchmove':
            case 'pointermove':
            case 'mousemove':
                this.handleMove(event);
                break;
            case 'touchend':
            case 'pointerup':
            case 'mouseup':
                this.handleEnd(event);
                break;
            case 'touchcancel':
            case 'pointercancel':
                this.handleCancel(event);
                break;
        }
    }
    
    handleStart(event) {
        const touches = this.extractTouches(event);
        const timestamp = performance.now();
        
        touches.forEach(touch => {
            const touchId = touch.identifier || 'mouse';
            
            this.touchState.touches.set(touchId, {
                startX: touch.clientX,
                startY: touch.clientY,
                currentX: touch.clientX,
                currentY: touch.clientY,
                startTime: timestamp,
                element: this.findTouchTarget(touch.clientX, touch.clientY),
                moved: false,
                distance: 0,
                velocity: { x: 0, y: 0 }
            });
        });
        
        // Start gesture detection
        this.detectGestureStart(touches, timestamp);
        
        // Provide visual feedback for touch
        this.addTouchFeedback(event.target);
        
        // Prevent scrolling on certain elements
        if (this.shouldPreventDefault(event.target)) {
            event.preventDefault();
        }
    }
    
    handleMove(event) {
        const touches = this.extractTouches(event);
        const timestamp = performance.now();
        
        touches.forEach(touch => {
            const touchId = touch.identifier || 'mouse';
            const touchData = this.touchState.touches.get(touchId);
            
            if (!touchData) return;
            
            const deltaX = touch.clientX - touchData.currentX;
            const deltaY = touch.clientY - touchData.currentY;
            const deltaTime = timestamp - touchData.startTime;
            
            // Update touch data
            touchData.currentX = touch.clientX;
            touchData.currentY = touch.clientY;
            touchData.moved = true;
            touchData.distance = Math.sqrt(
                Math.pow(touch.clientX - touchData.startX, 2) +
                Math.pow(touch.clientY - touchData.startY, 2)
            );
            
            // Calculate velocity
            if (deltaTime > 0) {
                touchData.velocity = {
                    x: deltaX / deltaTime,
                    y: deltaY / deltaTime
                };
            }
        });
        
        // Continue gesture detection
        this.detectGestureMove(touches, timestamp);
    }
    
    handleEnd(event) {
        const touches = this.extractTouches(event);
        const timestamp = performance.now();
        
        touches.forEach(touch => {
            const touchId = touch.identifier || 'mouse';
            const touchData = this.touchState.touches.get(touchId);
            
            if (!touchData) return;
            
            // Detect gestures
            this.detectGestureEnd(touchData, timestamp);
            
            // Remove touch data
            this.touchState.touches.delete(touchId);
        });
        
        // Clear visual feedback
        this.removeTouchFeedback(event.target);
    }
    
    handleCancel(event) {
        // Clear all touches
        this.touchState.touches.clear();
        this.clearAllGestures();
    }
    
    extractTouches(event) {
        if (event.touches) {
            return Array.from(event.touches);
        } else if (event.changedTouches) {
            return Array.from(event.changedTouches);
        } else {
            // Mouse event
            return [{
                clientX: event.clientX,
                clientY: event.clientY,
                identifier: 'mouse'
            }];
        }
    }
    
    findTouchTarget(x, y) {
        const element = document.elementFromPoint(x, y);
        return this.findInteractiveParent(element);
    }
    
    findInteractiveParent(element) {
        while (element && element !== document.body) {
            if (this.isInteractiveElement(element)) {
                return element;
            }
            element = element.parentElement;
        }
        return null;
    }
    
    isInteractiveElement(element) {
        const interactiveTags = ['button', 'a', 'input', 'textarea', 'select'];
        const interactiveRoles = ['button', 'link', 'menuitem', 'tab'];
        const interactiveClasses = ['btn', 'card', 'widget-card', 'clickable', 'touch-target'];
        
        return (
            interactiveTags.includes(element.tagName.toLowerCase()) ||
            interactiveRoles.includes(element.getAttribute('role')) ||
            interactiveClasses.some(cls => element.classList.contains(cls)) ||
            element.hasAttribute('data-touchable') ||
            element.onclick !== null
        );
    }
    
    detectGestureStart(touches, timestamp) {
        if (touches.length === 1) {
            // Single touch - could be tap, long press, or swipe
            const touch = touches[0];
            const touchData = this.touchState.touches.get(touch.identifier || 'mouse');
            
            // Start long press detection
            this.touchState.gestures.longPress = setTimeout(() => {
                if (this.touchState.touches.has(touch.identifier || 'mouse')) {
                    this.triggerLongPress(touchData);
                }
            }, this.touchState.settings.longPressTimeout);
            
        } else if (touches.length === 2) {
            // Two touches - could be pinch or two-finger pan
            this.startPinchGesture(touches);
        }
    }
    
    detectGestureMove(touches, timestamp) {
        if (touches.length === 1) {
            const touch = touches[0];
            const touchData = this.touchState.touches.get(touch.identifier || 'mouse');
            
            if (!touchData) return;
            
            // Cancel long press if moved too much
            if (touchData.distance > this.touchState.settings.panThreshold) {
                if (this.touchState.gestures.longPress) {
                    clearTimeout(this.touchState.gestures.longPress);
                    this.touchState.gestures.longPress = null;
                }
                
                // Start pan gesture
                this.continuePanGesture(touchData);
            }
            
        } else if (touches.length === 2) {
            // Continue pinch gesture
            this.continuePinchGesture(touches);
        }
    }
    
    detectGestureEnd(touchData, timestamp) {
        const duration = timestamp - touchData.startTime;
        const distance = touchData.distance;
        const velocity = Math.sqrt(touchData.velocity.x ** 2 + touchData.velocity.y ** 2);
        
        // Clear long press timer
        if (this.touchState.gestures.longPress) {
            clearTimeout(this.touchState.gestures.longPress);
            this.touchState.gestures.longPress = null;
        }
        
        // Detect tap
        if (!touchData.moved && duration < this.touchState.settings.tapTimeout) {
            this.triggerTap(touchData);
        }
        // Detect swipe
        else if (distance > this.touchState.settings.swipeThreshold && 
                 velocity > this.touchState.settings.swipeVelocityThreshold) {
            this.triggerSwipe(touchData);
        }
        // Detect pan end
        else if (touchData.moved) {
            this.triggerPanEnd(touchData);
        }
    }
    
    triggerTap(touchData) {
        const event = new CustomEvent('customTap', {
            detail: {
                x: touchData.startX,
                y: touchData.startY,
                target: touchData.element,
                timestamp: performance.now()
            }
        });
        
        if (touchData.element) {
            touchData.element.dispatchEvent(event);
            
            // Add tap animation
            this.addTapAnimation(touchData.element);
            
            // Trigger haptic feedback
            this.triggerHapticFeedback('light');
        }
    }
    
    triggerLongPress(touchData) {
        const event = new CustomEvent('customLongPress', {
            detail: {
                x: touchData.startX,
                y: touchData.startY,
                target: touchData.element,
                timestamp: performance.now()
            }
        });
        
        if (touchData.element) {
            touchData.element.dispatchEvent(event);
            
            // Add long press animation
            this.addLongPressAnimation(touchData.element);
            
            // Trigger stronger haptic feedback
            this.triggerHapticFeedback('medium');
        }
    }
    
    triggerSwipe(touchData) {
        const deltaX = touchData.currentX - touchData.startX;
        const deltaY = touchData.currentY - touchData.startY;
        
        let direction = 'unknown';
        if (Math.abs(deltaX) > Math.abs(deltaY)) {
            direction = deltaX > 0 ? 'right' : 'left';
        } else {
            direction = deltaY > 0 ? 'down' : 'up';
        }
        
        const event = new CustomEvent('customSwipe', {
            detail: {
                direction,
                distance: touchData.distance,
                velocity: touchData.velocity,
                startX: touchData.startX,
                startY: touchData.startY,
                endX: touchData.currentX,
                endY: touchData.currentY,
                target: touchData.element,
                timestamp: performance.now()
            }
        });
        
        if (touchData.element) {
            touchData.element.dispatchEvent(event);
        }
        
        document.dispatchEvent(event);
    }
    
    startPinchGesture(touches) {
        if (touches.length !== 2) return;
        
        const [touch1, touch2] = touches;
        const distance = Math.sqrt(
            Math.pow(touch2.clientX - touch1.clientX, 2) +
            Math.pow(touch2.clientY - touch1.clientY, 2)
        );
        
        this.touchState.gestures.pinch = {
            initialDistance: distance,
            currentDistance: distance,
            centerX: (touch1.clientX + touch2.clientX) / 2,
            centerY: (touch1.clientY + touch2.clientY) / 2
        };
    }
    
    continuePinchGesture(touches) {
        if (!this.touchState.gestures.pinch || touches.length !== 2) return;
        
        const [touch1, touch2] = touches;
        const distance = Math.sqrt(
            Math.pow(touch2.clientX - touch1.clientX, 2) +
            Math.pow(touch2.clientY - touch1.clientY, 2)
        );
        
        const scale = distance / this.touchState.gestures.pinch.initialDistance;
        const centerX = (touch1.clientX + touch2.clientX) / 2;
        const centerY = (touch1.clientY + touch2.clientY) / 2;
        
        const event = new CustomEvent('customPinch', {
            detail: {
                scale,
                centerX,
                centerY,
                distance,
                initialDistance: this.touchState.gestures.pinch.initialDistance,
                timestamp: performance.now()
            }
        });
        
        document.dispatchEvent(event);
    }
    
    continuePanGesture(touchData) {
        const event = new CustomEvent('customPan', {
            detail: {
                deltaX: touchData.currentX - touchData.startX,
                deltaY: touchData.currentY - touchData.startY,
                currentX: touchData.currentX,
                currentY: touchData.currentY,
                startX: touchData.startX,
                startY: touchData.startY,
                velocity: touchData.velocity,
                target: touchData.element,
                timestamp: performance.now()
            }
        });
        
        if (touchData.element) {
            touchData.element.dispatchEvent(event);
        }
    }
    
    triggerPanEnd(touchData) {
        const event = new CustomEvent('customPanEnd', {
            detail: {
                deltaX: touchData.currentX - touchData.startX,
                deltaY: touchData.currentY - touchData.startY,
                velocity: touchData.velocity,
                target: touchData.element,
                timestamp: performance.now()
            }
        });
        
        if (touchData.element) {
            touchData.element.dispatchEvent(event);
        }
    }
    
    addTouchFeedback(element) {
        if (!element || !this.isInteractiveElement(element)) return;
        
        element.classList.add('touch-active');
        
        // Remove after animation
        setTimeout(() => {
            element.classList.remove('touch-active');
        }, 200);
    }
    
    removeTouchFeedback(element) {
        if (!element) return;
        element.classList.remove('touch-active');
    }
    
    addTapAnimation(element) {
        if (!element) return;
        
        element.style.transform = 'scale(0.98)';
        element.style.transition = 'transform 0.1s ease';
        
        setTimeout(() => {
            element.style.transform = '';
            element.style.transition = '';
        }, 100);
    }
    
    addLongPressAnimation(element) {
        if (!element) return;
        
        element.classList.add('long-press-active');
        
        setTimeout(() => {
            element.classList.remove('long-press-active');
        }, 300);
    }
    
    enhanceTouchTargets() {
        // Find all interactive elements and enhance their touch targets
        const interactiveElements = document.querySelectorAll('button, a, input, select, textarea, [role="button"], [data-touchable]');
        
        interactiveElements.forEach(element => {
            this.enhanceElement(element);
        });
        
        // Use MutationObserver to enhance dynamically added elements
        const observer = new MutationObserver((mutations) => {
            mutations.forEach(mutation => {
                mutation.addedNodes.forEach(node => {
                    if (node.nodeType === 1) { // Element node
                        if (this.isInteractiveElement(node)) {
                            this.enhanceElement(node);
                        }
                        
                        // Check children
                        const children = node.querySelectorAll && node.querySelectorAll('button, a, input, select, textarea, [role="button"], [data-touchable]');
                        if (children) {
                            children.forEach(child => this.enhanceElement(child));
                        }
                    }
                });
            });
        });
        
        observer.observe(document.body, {
            childList: true,
            subtree: true
        });
    }
    
    enhanceElement(element) {
        if (this.touchTargets.has(element)) return;
        
        // Calculate current dimensions
        const rect = element.getBoundingClientRect();
        const minSize = this.touchState.settings.minTouchTarget || 44;
        
        // Add touch enhancement class
        element.classList.add('touch-enhanced');
        
        // Store original styles
        const originalStyles = {
            minWidth: element.style.minWidth,
            minHeight: element.style.minHeight,
            padding: element.style.padding
        };
        
        this.touchTargets.set(element, originalStyles);
        
        // Ensure minimum touch target size
        if (rect.width < minSize || rect.height < minSize) {
            const paddingX = Math.max(0, (minSize - rect.width) / 2);
            const paddingY = Math.max(0, (minSize - rect.height) / 2);
            
            element.style.minWidth = `${minSize}px`;
            element.style.minHeight = `${minSize}px`;
            element.style.padding = `${paddingY}px ${paddingX}px`;
        }
        
        // Add custom touch event listeners
        this.addCustomEventListeners(element);
    }
    
    addCustomEventListeners(element) {
        // Handle custom gestures
        element.addEventListener('customTap', (e) => {
            if (element.hasAttribute('data-tap-action')) {
                const action = element.getAttribute('data-tap-action');
                this.executeAction(action, element, e.detail);
            }
        });
        
        element.addEventListener('customLongPress', (e) => {
            if (element.hasAttribute('data-longpress-action')) {
                const action = element.getAttribute('data-longpress-action');
                this.executeAction(action, element, e.detail);
            } else {
                // Default long press behavior - show context menu
                this.showContextMenu(element, e.detail.x, e.detail.y);
            }
        });
        
        element.addEventListener('customSwipe', (e) => {
            if (element.hasAttribute('data-swipe-action')) {
                const action = element.getAttribute('data-swipe-action');
                this.executeAction(action, element, e.detail);
            }
        });
    }
    
    executeAction(action, element, detail) {
        switch (action) {
            case 'refresh':
                if (window.dashboard && window.dashboard.refreshSpecificWidget) {
                    const widgetId = element.closest('.widget-card')?.dataset.widgetId;
                    if (widgetId) {
                        window.dashboard.refreshSpecificWidget(widgetId);
                    }
                }
                break;
            case 'configure':
                if (window.widgetManager && window.widgetManager.openWidgetConfig) {
                    const widgetId = element.closest('.widget-card')?.dataset.widgetId;
                    if (widgetId) {
                        window.widgetManager.openWidgetConfig(widgetId);
                    }
                }
                break;
            case 'toggle':
                element.classList.toggle('active');
                break;
            default:
                // Try to execute as JavaScript
                try {
                    new Function('element', 'detail', action)(element, detail);
                } catch (error) {
                    console.warn('Failed to execute touch action:', action, error);
                }
        }
    }
    
    showContextMenu(element, x, y) {
        // Remove any existing context menu
        const existingMenu = document.querySelector('.touch-context-menu');
        if (existingMenu) {
            existingMenu.remove();
        }
        
        const menu = document.createElement('div');
        menu.className = 'touch-context-menu';
        menu.style.cssText = `
            position: fixed;
            left: ${x}px;
            top: ${y}px;
            background: var(--bg-secondary);
            border: 1px solid var(--border-primary);
            border-radius: var(--radius-lg);
            box-shadow: var(--shadow-xl);
            z-index: var(--z-popover);
            padding: var(--space-2) 0;
            min-width: 150px;
            animation: fadeIn 0.2s ease;
        `;
        
        // Add menu items based on element context
        const menuItems = this.getContextMenuItems(element);
        
        menuItems.forEach(item => {
            const menuItem = document.createElement('div');
            menuItem.className = 'touch-context-menu-item';
            menuItem.textContent = item.label;
            menuItem.style.cssText = `
                padding: var(--space-3) var(--space-4);
                cursor: pointer;
                color: var(--text-primary);
                font-size: var(--font-size-sm);
                transition: background-color var(--duration-fast) var(--ease-in-out);
                min-height: var(--touch-target-min);
                display: flex;
                align-items: center;
            `;
            
            menuItem.addEventListener('click', () => {
                item.action();
                menu.remove();
            });
            
            menuItem.addEventListener('touchstart', (e) => {
                e.stopPropagation();
                menuItem.style.backgroundColor = 'var(--bg-tertiary)';
            });
            
            menu.appendChild(menuItem);
        });
        
        document.body.appendChild(menu);
        
        // Position menu within viewport
        this.positionContextMenu(menu, x, y);
        
        // Close menu when touching elsewhere
        const closeMenu = (e) => {
            if (!menu.contains(e.target)) {
                menu.remove();
                document.removeEventListener('touchstart', closeMenu);
                document.removeEventListener('click', closeMenu);
            }
        };
        
        setTimeout(() => {
            document.addEventListener('touchstart', closeMenu);
            document.addEventListener('click', closeMenu);
        }, 100);
    }
    
    getContextMenuItems(element) {
        const items = [];
        
        // Widget-specific items
        if (element.closest('.widget-card')) {
            const widgetId = element.closest('.widget-card').dataset.widgetId;
            items.push(
                { label: 'Refresh', action: () => window.dashboard.refreshSpecificWidget(widgetId) },
                { label: 'Configure', action: () => window.widgetManager.openWidgetConfig(widgetId) },
                { label: 'Remove', action: () => window.widgetManager.removeWidget(widgetId) }
            );
        }
        
        // Button-specific items
        if (element.tagName === 'BUTTON') {
            items.push(
                { label: 'Activate', action: () => element.click() }
            );
        }
        
        // Link-specific items
        if (element.tagName === 'A') {
            items.push(
                { label: 'Open', action: () => element.click() },
                { label: 'Copy Link', action: () => this.copyToClipboard(element.href) }
            );
        }
        
        // Default items
        if (items.length === 0) {
            items.push(
                { label: 'Inspect Element', action: () => console.log(element) }
            );
        }
        
        return items;
    }
    
    positionContextMenu(menu, x, y) {
        const rect = menu.getBoundingClientRect();
        const viewport = {
            width: window.innerWidth,
            height: window.innerHeight
        };
        
        let left = x;
        let top = y;
        
        // Adjust horizontal position
        if (left + rect.width > viewport.width) {
            left = viewport.width - rect.width - 10;
        }
        if (left < 10) {
            left = 10;
        }
        
        // Adjust vertical position
        if (top + rect.height > viewport.height) {
            top = y - rect.height - 10;
        }
        if (top < 10) {
            top = 10;
        }
        
        menu.style.left = `${left}px`;
        menu.style.top = `${top}px`;
    }
    
    initializeHapticFeedback() {
        this.hapticSettings = {
            enabled: this.deviceCapabilities.hasHapticFeedback && 
                     localStorage.getItem('haptic-feedback') !== 'disabled',
            patterns: {
                light: [10],
                medium: [50],
                strong: [100],
                double: [30, 30, 30],
                success: [10, 10, 10, 100],
                error: [100, 50, 100]
            }
        };
    }
    
    triggerHapticFeedback(pattern = 'light') {
        if (!this.hapticSettings.enabled || !navigator.vibrate) return;
        
        const vibrationPattern = this.hapticSettings.patterns[pattern] || [10];
        navigator.vibrate(vibrationPattern);
    }
    
    setupAccessibilityFeatures() {
        // Add ARIA labels for touch interactions
        document.addEventListener('DOMContentLoaded', () => {
            this.enhanceAccessibility();
        });
        
        // Handle keyboard navigation for touch-enhanced elements
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Enter' || e.key === ' ') {
                const element = e.target;
                if (element.classList.contains('touch-enhanced')) {
                    e.preventDefault();
                    this.simulateTap(element);
                }
            }
        });
    }
    
    enhanceAccessibility() {
        const touchElements = document.querySelectorAll('.touch-enhanced');
        
        touchElements.forEach(element => {
            // Ensure focusability
            if (!element.hasAttribute('tabindex') && element.tagName !== 'BUTTON' && element.tagName !== 'A') {
                element.setAttribute('tabindex', '0');
            }
            
            // Add ARIA labels for gesture capabilities
            const gestures = [];
            if (element.hasAttribute('data-tap-action')) gestures.push('tap');
            if (element.hasAttribute('data-longpress-action')) gestures.push('long press');
            if (element.hasAttribute('data-swipe-action')) gestures.push('swipe');
            
            if (gestures.length > 0) {
                const gestureText = gestures.join(', ');
                element.setAttribute('aria-description', `Supports: ${gestureText}`);
            }
        });
    }
    
    simulateTap(element) {
        const rect = element.getBoundingClientRect();
        const x = rect.left + rect.width / 2;
        const y = rect.top + rect.height / 2;
        
        const touchData = {
            startX: x,
            startY: y,
            currentX: x,
            currentY: y,
            startTime: performance.now(),
            element: element,
            moved: false,
            distance: 0,
            velocity: { x: 0, y: 0 }
        };
        
        this.triggerTap(touchData);
    }
    
    handleContextMenu(event) {
        // Prevent default context menu on touch devices for better UX
        if (this.touchState.isTouch) {
            event.preventDefault();
        }
    }
    
    shouldPreventDefault(element) {
        // Prevent default behavior for certain elements to improve touch UX
        const preventElements = ['button', '.btn', '.widget-card', '[data-touchable]'];
        
        return preventElements.some(selector => {
            if (selector.startsWith('.')) {
                return element.classList.contains(selector.slice(1));
            } else if (selector.startsWith('[')) {
                const attr = selector.slice(1, -1);
                return element.hasAttribute(attr);
            } else {
                return element.tagName.toLowerCase() === selector;
            }
        });
    }
    
    clearAllGestures() {
        if (this.touchState.gestures.longPress) {
            clearTimeout(this.touchState.gestures.longPress);
            this.touchState.gestures.longPress = null;
        }
        
        this.touchState.gestures.pinch = null;
        this.touchState.gestures.pan = null;
        this.touchState.gestures.swipe = null;
    }
    
    copyToClipboard(text) {
        if (navigator.clipboard) {
            navigator.clipboard.writeText(text).then(() => {
                this.showToast('Link copied to clipboard', 'success');
            });
        }
    }
    
    showToast(message, type = 'info') {
        const toast = document.createElement('div');
        toast.className = `toast toast-${type}`;
        toast.innerHTML = `
            <div class="toast-content">
                <div class="toast-message">${message}</div>
            </div>
        `;
        
        let container = document.querySelector('.toast-container');
        if (!container) {
            container = document.createElement('div');
            container.className = 'toast-container';
            document.body.appendChild(container);
        }
        
        container.appendChild(toast);
        
        setTimeout(() => {
            toast.remove();
        }, 3000);
    }
    
    // Public API methods
    enableHapticFeedback() {
        this.hapticSettings.enabled = true;
        localStorage.setItem('haptic-feedback', 'enabled');
    }
    
    disableHapticFeedback() {
        this.hapticSettings.enabled = false;
        localStorage.setItem('haptic-feedback', 'disabled');
    }
    
    getDeviceCapabilities() {
        return { ...this.deviceCapabilities };
    }
    
    getTouchState() {
        return {
            isTouch: this.touchState.isTouch,
            activeTouches: this.touchState.touches.size,
            hasActiveGestures: Object.values(this.touchState.gestures).some(g => g !== null)
        };
    }
    
    destroy() {
        // Clean up event listeners and timers
        this.clearAllGestures();
        this.touchState.touches.clear();
        
        if (this.rafId) {
            cancelAnimationFrame(this.rafId);
        }
        
        document.documentElement.classList.remove('touch-device');
    }
}

// CSS for touch feedback animations
const touchStyles = document.createElement('style');
touchStyles.textContent = `
    .touch-active {
        background-color: rgba(255, 255, 255, 0.1) !important;
        transform: scale(0.98);
        transition: all 0.1s ease;
    }
    
    .long-press-active {
        animation: longPressAnimation 0.3s ease;
    }
    
    @keyframes longPressAnimation {
        0% { transform: scale(1); }
        50% { transform: scale(1.05); }
        100% { transform: scale(1); }
    }
    
    .touch-enhanced {
        position: relative;
        cursor: pointer;
    }
    
    .touch-enhanced::before {
        content: '';
        position: absolute;
        inset: -8px;
        border-radius: inherit;
        pointer-events: none;
        z-index: -1;
    }
    
    @media (hover: none) {
        .touch-enhanced:hover {
            transform: none !important;
        }
    }
`;

document.head.appendChild(touchStyles);

// Initialize touch interaction manager
let touchManager;

// Initialize when DOM is ready
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', () => {
        touchManager = new TouchInteractionManager();
        window.touchManager = touchManager;
    });
} else {
    touchManager = new TouchInteractionManager();
    window.touchManager = touchManager;
}

// Export for module systems
if (typeof module !== 'undefined' && module.exports) {
    module.exports = TouchInteractionManager;
} else if (typeof window !== 'undefined') {
    window.TouchInteractionManager = TouchInteractionManager;
}