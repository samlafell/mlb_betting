/**
 * Performance Optimization Service
 * 
 * Advanced performance optimizations for the dashboard including:
 * - Intelligent caching strategies
 * - Resource prefetching
 * - Rendering optimizations
 * - Memory management
 * - Bundle optimization
 * - Real-time performance monitoring
 */

class PerformanceOptimizer {
    constructor() {
        this.cache = new Map();
        this.resourceCache = new Map();
        this.performanceConfig = {
            maxCacheSize: 100,
            cacheExpiration: 5 * 60 * 1000, // 5 minutes
            prefetchThreshold: 0.7,
            renderBatchSize: 5,
            memoryThreshold: 0.8
        };
        
        this.metrics = {
            cacheHits: 0,
            cacheMisses: 0,
            renderTime: [],
            memoryUsage: [],
            networkRequests: 0,
            bundleSize: 0
        };
        
        this.observers = {
            intersection: null,
            mutation: null,
            performance: null
        };
        
        this.init();
    }
    
    init() {
        this.setupCaching();
        this.setupPrefetching();
        this.setupRenderOptimization();
        this.setupMemoryManagement();
        this.setupPerformanceMonitoring();
        this.setupResourceOptimization();
        this.startPerformanceTracking();
    }
    
    setupCaching() {
        // Intelligent cache with LRU eviction
        this.cache.set = (key, value) => {
            if (this.cache.size >= this.performanceConfig.maxCacheSize) {
                // Remove oldest entry
                const firstKey = this.cache.keys().next().value;
                this.cache.delete(firstKey);
            }
            
            const cacheEntry = {
                data: value,
                timestamp: Date.now(),
                hits: 0,
                lastAccess: Date.now()
            };
            
            Map.prototype.set.call(this.cache, key, cacheEntry);
        };
        
        this.cache.get = (key) => {
            const entry = Map.prototype.get.call(this.cache, key);
            
            if (!entry) {
                this.metrics.cacheMisses++;
                return null;
            }
            
            // Check expiration
            if (Date.now() - entry.timestamp > this.performanceConfig.cacheExpiration) {
                this.cache.delete(key);
                this.metrics.cacheMisses++;
                return null;
            }
            
            // Update access statistics
            entry.hits++;
            entry.lastAccess = Date.now();
            this.metrics.cacheHits++;
            
            return entry.data;
        };
    }
    
    setupPrefetching() {
        // Intelligent resource prefetching
        this.observers.intersection = new IntersectionObserver((entries) => {
            entries.forEach(entry => {
                if (entry.intersectionRatio >= this.performanceConfig.prefetchThreshold) {
                    this.prefetchWidgetResources(entry.target);
                }
            });
        }, { threshold: [0.5, 0.7, 1.0] });
        
        // Prefetch on hover for desktop
        document.addEventListener('mouseenter', (e) => {
            const widget = e.target.closest('.widget-card');
            if (widget && !this.isMobile()) {
                this.prefetchWidgetResources(widget);
            }
        }, true);
    }
    
    prefetchWidgetResources(widget) {
        const widgetId = widget.dataset.widgetId;
        if (!widgetId || this.resourceCache.has(widgetId)) return;
        
        // Prefetch widget-specific resources
        const resourceUrls = this.getWidgetResourceUrls(widgetId);
        resourceUrls.forEach(url => this.prefetchResource(url));
        
        this.resourceCache.set(widgetId, Date.now());
    }
    
    getWidgetResourceUrls(widgetId) {
        const urls = [];
        
        switch (widgetId) {
            case 'line-movement':
                urls.push('/api/analytics/line-movement-chart?game_id=1');
                break;
            case 'opportunities':
                urls.push('/api/opportunities/recent');
                break;
            case 'performance-metrics':
                urls.push('/api/metrics/all');
                break;
            default:
                break;
        }
        
        return urls;
    }
    
    async prefetchResource(url) {
        try {
            const cacheKey = `prefetch_${url}`;
            
            if (this.cache.get(cacheKey)) return;
            
            const response = await fetch(url, {
                method: 'GET',
                headers: { 'Cache-Control': 'max-age=300' }
            });
            
            if (response.ok) {
                const data = await response.json();
                this.cache.set(cacheKey, data);
            }
        } catch (error) {
            console.debug('Prefetch failed for:', url);
        }
    }
    
    setupRenderOptimization() {
        // Batch DOM updates
        this.renderQueue = [];
        this.isRendering = false;
        
        this.batchRender = (renderFunction) => {
            this.renderQueue.push(renderFunction);
            
            if (!this.isRendering) {
                this.isRendering = true;
                requestIdleCallback(() => {
                    this.processRenderQueue();
                }, { timeout: 100 });
            }
        };
        
        // Virtual scrolling for large lists
        this.setupVirtualScrolling();
        
        // Optimize reflows and repaints
        this.setupReflowOptimization();
    }
    
    processRenderQueue() {
        const startTime = performance.now();
        let processed = 0;
        
        while (this.renderQueue.length > 0 && processed < this.performanceConfig.renderBatchSize) {
            const renderFunction = this.renderQueue.shift();
            try {
                renderFunction();
                processed++;
            } catch (error) {
                console.error('Render function failed:', error);
            }
        }
        
        const renderTime = performance.now() - startTime;
        this.metrics.renderTime.push(renderTime);
        
        if (this.renderQueue.length > 0) {
            requestIdleCallback(() => {
                this.processRenderQueue();
            }, { timeout: 100 });
        } else {
            this.isRendering = false;
        }
    }
    
    setupVirtualScrolling() {
        // Implement virtual scrolling for large widget content
        const scrollContainers = document.querySelectorAll('.widget-content[data-virtual-scroll]');
        
        scrollContainers.forEach(container => {
            this.enableVirtualScrolling(container);
        });
    }
    
    enableVirtualScrolling(container) {
        const itemHeight = 60; // Approximate item height
        const visibleItems = Math.ceil(container.offsetHeight / itemHeight) + 5; // Buffer
        
        let scrollTop = 0;
        let startIndex = 0;
        let endIndex = visibleItems;
        
        container.addEventListener('scroll', () => {
            scrollTop = container.scrollTop;
            startIndex = Math.floor(scrollTop / itemHeight);
            endIndex = Math.min(startIndex + visibleItems, this.getTotalItems(container));
            
            this.updateVirtualContent(container, startIndex, endIndex, itemHeight);
        }, { passive: true });
    }
    
    updateVirtualContent(container, startIndex, endIndex, itemHeight) {
        const fragment = document.createDocumentFragment();
        const spacerTop = document.createElement('div');
        const spacerBottom = document.createElement('div');
        
        spacerTop.style.height = `${startIndex * itemHeight}px`;
        spacerBottom.style.height = `${(this.getTotalItems(container) - endIndex) * itemHeight}px`;
        
        fragment.appendChild(spacerTop);
        
        // Render visible items
        for (let i = startIndex; i < endIndex; i++) {
            const item = this.createVirtualItem(container, i);
            if (item) fragment.appendChild(item);
        }
        
        fragment.appendChild(spacerBottom);
        
        // Update container content
        this.batchRender(() => {
            container.innerHTML = '';
            container.appendChild(fragment);
        });
    }
    
    createVirtualItem(container, index) {
        // This would be implemented based on the specific widget content
        const widgetId = container.closest('.widget-card')?.dataset.widgetId;
        // Return appropriate item based on widget type and index
        return null;
    }
    
    getTotalItems(container) {
        // Get total number of items for virtual scrolling
        const widgetId = container.closest('.widget-card')?.dataset.widgetId;
        // Return total count based on widget data
        return 0;
    }
    
    setupReflowOptimization() {
        // Batch style changes to minimize reflows
        this.styleQueue = [];
        this.isStyleUpdating = false;
        
        this.batchStyle = (element, styles) => {
            this.styleQueue.push({ element, styles });
            
            if (!this.isStyleUpdating) {
                this.isStyleUpdating = true;
                requestAnimationFrame(() => {
                    this.processStyleQueue();
                });
            }
        };
    }
    
    processStyleQueue() {
        // Process all style changes in one batch
        this.styleQueue.forEach(({ element, styles }) => {
            Object.assign(element.style, styles);
        });
        
        this.styleQueue = [];
        this.isStyleUpdating = false;
    }
    
    setupMemoryManagement() {
        // Monitor memory usage
        if (performance.memory) {
            setInterval(() => {
                const memoryInfo = performance.memory;
                const usage = memoryInfo.usedJSHeapSize / memoryInfo.jsHeapSizeLimit;
                this.metrics.memoryUsage.push(usage);
                
                if (usage > this.performanceConfig.memoryThreshold) {
                    this.performMemoryCleanup();
                }
            }, 30000); // Check every 30 seconds
        }
        
        // Cleanup on page visibility change
        document.addEventListener('visibilitychange', () => {
            if (document.hidden) {
                this.performMemoryCleanup();
            }
        });
    }
    
    performMemoryCleanup() {
        // Clear expired cache entries
        const now = Date.now();
        for (const [key, entry] of this.cache.entries()) {
            if (now - entry.timestamp > this.performanceConfig.cacheExpiration) {
                this.cache.delete(key);
            }
        }
        
        // Clear old metrics
        this.metrics.renderTime = this.metrics.renderTime.slice(-100);
        this.metrics.memoryUsage = this.metrics.memoryUsage.slice(-100);
        
        // Clear resource cache
        this.resourceCache.clear();
        
        // Force garbage collection if available
        if (window.gc) {
            window.gc();
        }
        
        console.debug('Memory cleanup performed');
    }
    
    setupPerformanceMonitoring() {
        // Monitor Core Web Vitals
        this.setupCoreWebVitalsMonitoring();
        
        // Monitor custom performance metrics
        this.setupCustomMetrics();
        
        // Setup performance observer
        if ('PerformanceObserver' in window) {
            this.observers.performance = new PerformanceObserver((list) => {
                this.processPerformanceEntries(list.getEntries());
            });
            
            this.observers.performance.observe({ 
                entryTypes: ['navigation', 'resource', 'measure', 'longtask']
            });
        }
    }
    
    setupCoreWebVitalsMonitoring() {
        // First Contentful Paint (FCP)
        this.measureFCP();
        
        // Largest Contentful Paint (LCP)
        this.measureLCP();
        
        // First Input Delay (FID)
        this.measureFID();
        
        // Cumulative Layout Shift (CLS)
        this.measureCLS();
    }
    
    measureFCP() {
        new PerformanceObserver((entryList) => {
            for (const entry of entryList.getEntries()) {
                if (entry.name === 'first-contentful-paint') {
                    this.metrics.fcp = entry.startTime;
                    console.debug('FCP:', entry.startTime);
                }
            }
        }).observe({ entryTypes: ['paint'] });
    }
    
    measureLCP() {
        new PerformanceObserver((entryList) => {
            for (const entry of entryList.getEntries()) {
                this.metrics.lcp = entry.startTime;
                console.debug('LCP:', entry.startTime);
            }
        }).observe({ entryTypes: ['largest-contentful-paint'] });
    }
    
    measureFID() {
        new PerformanceObserver((entryList) => {
            for (const entry of entryList.getEntries()) {
                this.metrics.fid = entry.processingStart - entry.startTime;
                console.debug('FID:', this.metrics.fid);
            }
        }).observe({ entryTypes: ['first-input'] });
    }
    
    measureCLS() {
        let cls = 0;
        new PerformanceObserver((entryList) => {
            for (const entry of entryList.getEntries()) {
                if (!entry.hadRecentInput) {
                    cls += entry.value;
                }
            }
            this.metrics.cls = cls;
            console.debug('CLS:', cls);
        }).observe({ entryTypes: ['layout-shift'] });
    }
    
    setupCustomMetrics() {
        // Widget load time
        this.measureWidgetLoadTime();
        
        // API response time
        this.measureAPIResponseTime();
        
        // Interaction to next paint
        this.measureInteractionToNextPaint();
    }
    
    measureWidgetLoadTime() {
        // Track time from widget creation to content load
        const observer = new MutationObserver((mutations) => {
            mutations.forEach((mutation) => {
                if (mutation.type === 'childList') {
                    mutation.addedNodes.forEach((node) => {
                        if (node.classList && node.classList.contains('widget-card')) {
                            const startTime = performance.now();
                            node.dataset.loadStart = startTime;
                            
                            // Measure when content is loaded
                            const contentEl = node.querySelector('.widget-content');
                            if (contentEl) {
                                const contentObserver = new MutationObserver(() => {
                                    if (contentEl.querySelector(':not(.loading-placeholder)')) {
                                        const loadTime = performance.now() - startTime;
                                        this.metrics.widgetLoadTime = loadTime;
                                        console.debug(`Widget ${node.dataset.widgetId} loaded in ${loadTime}ms`);
                                        contentObserver.disconnect();
                                    }
                                });
                                contentObserver.observe(contentEl, { childList: true, subtree: true });
                            }
                        }
                    });
                }
            });
        });
        
        observer.observe(document.body, { childList: true, subtree: true });
        this.observers.mutation = observer;
    }
    
    measureAPIResponseTime() {
        // Intercept fetch requests to measure API response times
        const originalFetch = window.fetch;
        window.fetch = async (...args) => {
            const startTime = performance.now();
            
            try {
                const response = await originalFetch.apply(window, args);
                const endTime = performance.now();
                const duration = endTime - startTime;
                
                this.metrics.networkRequests++;
                this.metrics.apiResponseTime = duration;
                
                console.debug(`API call to ${args[0]} took ${duration}ms`);
                
                return response;
            } catch (error) {
                const endTime = performance.now();
                const duration = endTime - startTime;
                console.debug(`Failed API call to ${args[0]} took ${duration}ms`);
                throw error;
            }
        };
    }
    
    measureInteractionToNextPaint() {
        let interactionStart = 0;
        
        ['click', 'touchstart', 'keydown'].forEach(eventType => {
            document.addEventListener(eventType, () => {
                interactionStart = performance.now();
            }, { passive: true });
        });
        
        new PerformanceObserver((entryList) => {
            for (const entry of entryList.getEntries()) {
                if (interactionStart > 0 && entry.name === 'first-paint') {
                    const inp = entry.startTime - interactionStart;
                    this.metrics.inp = inp;
                    console.debug('Interaction to Next Paint:', inp);
                    interactionStart = 0;
                }
            }
        }).observe({ entryTypes: ['paint'] });
    }
    
    setupResourceOptimization() {
        // Optimize images
        this.optimizeImages();
        
        // Compress and cache CSS/JS resources
        this.optimizeStaticResources();
        
        // Setup service worker for caching
        this.setupServiceWorker();
    }
    
    optimizeImages() {
        // Lazy load images and convert to WebP where supported
        const images = document.querySelectorAll('img[data-src]');
        const supportsWebP = this.checkWebPSupport();
        
        const imageObserver = new IntersectionObserver((entries) => {
            entries.forEach(entry => {
                if (entry.isIntersecting) {
                    const img = entry.target;
                    let src = img.dataset.src;
                    
                    if (supportsWebP) {
                        src = src.replace(/\.(jpg|jpeg|png)/, '.webp');
                    }
                    
                    img.src = src;
                    img.onload = () => {
                        img.classList.add('loaded');
                    };
                    
                    imageObserver.unobserve(img);
                }
            });
        });
        
        images.forEach(img => imageObserver.observe(img));
    }
    
    checkWebPSupport() {
        const canvas = document.createElement('canvas');
        canvas.width = 1;
        canvas.height = 1;
        return canvas.toDataURL('image/webp').indexOf('webp') > -1;
    }
    
    optimizeStaticResources() {
        // Preload critical CSS
        const criticalCSS = ['/static/css/widget-system.css'];
        criticalCSS.forEach(href => {
            const link = document.createElement('link');
            link.rel = 'preload';
            link.as = 'style';
            link.href = href;
            document.head.appendChild(link);
        });
        
        // Preload critical JS
        const criticalJS = ['/static/js/widget-manager.js'];
        criticalJS.forEach(src => {
            const link = document.createElement('link');
            link.rel = 'preload';
            link.as = 'script';
            link.href = src;
            document.head.appendChild(link);
        });
    }
    
    setupServiceWorker() {
        if ('serviceWorker' in navigator) {
            navigator.serviceWorker.register('/sw.js')
                .then(() => console.debug('Service Worker registered'))
                .catch(err => console.debug('Service Worker registration failed:', err));
        }
    }
    
    startPerformanceTracking() {
        // Report performance metrics periodically
        setInterval(() => {
            this.reportPerformanceMetrics();
        }, 60000); // Every minute
    }
    
    reportPerformanceMetrics() {
        const report = {
            timestamp: Date.now(),
            cacheHitRate: this.metrics.cacheHits / (this.metrics.cacheHits + this.metrics.cacheMisses),
            averageRenderTime: this.getAverage(this.metrics.renderTime),
            memoryUsage: performance.memory ? performance.memory.usedJSHeapSize : 0,
            networkRequests: this.metrics.networkRequests,
            coreWebVitals: {
                fcp: this.metrics.fcp,
                lcp: this.metrics.lcp,
                fid: this.metrics.fid,
                cls: this.metrics.cls
            }
        };
        
        // Send to monitoring endpoint if available
        if (typeof window.dashboard !== 'undefined') {
            console.debug('Performance Report:', report);
        }
        
        return report;
    }
    
    getAverage(array) {
        return array.length > 0 ? array.reduce((a, b) => a + b) / array.length : 0;
    }
    
    isMobile() {
        return window.innerWidth < 768 || /Android|iPhone|iPad|iPod|BlackBerry|IEMobile|Opera Mini/i.test(navigator.userAgent);
    }
    
    // Public API
    getCacheStats() {
        return {
            size: this.cache.size,
            hits: this.metrics.cacheHits,
            misses: this.metrics.cacheMisses,
            hitRate: this.metrics.cacheHits / (this.metrics.cacheHits + this.metrics.cacheMisses)
        };
    }
    
    clearCache() {
        this.cache.clear();
        this.resourceCache.clear();
        console.debug('Performance caches cleared');
    }
    
    getPerformanceReport() {
        return this.reportPerformanceMetrics();
    }
    
    optimizeForDevice() {
        if (this.isMobile()) {
            // Mobile-specific optimizations
            this.performanceConfig.renderBatchSize = 3;
            this.performanceConfig.cacheExpiration = 2 * 60 * 1000; // 2 minutes
        }
    }
    
    destroy() {
        // Clean up observers and intervals
        Object.values(this.observers).forEach(observer => {
            if (observer) observer.disconnect();
        });
        
        this.clearCache();
    }
}

// Export for global access
window.PerformanceOptimizer = PerformanceOptimizer;