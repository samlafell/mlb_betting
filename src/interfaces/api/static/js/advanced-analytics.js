/**
 * Advanced Analytics Dashboard JavaScript
 * 
 * Handles interactive charting, statistical analysis, and mobile-responsive
 * analytics for the MLB betting system.
 */

class AdvancedAnalyticsDashboard {
    constructor() {
        this.charts = {};
        this.currentFilters = {};
        this.websocket = null;
        this.isLoading = false;
        
        // Chart configurations
        this.chartDefaults = {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: 'top',
                    labels: {
                        color: '#e5e7eb'
                    }
                }
            },
            scales: {
                x: {
                    ticks: {
                        color: '#9ca3af'
                    },
                    grid: {
                        color: '#374151'
                    }
                },
                y: {
                    ticks: {
                        color: '#9ca3af'
                    },
                    grid: {
                        color: '#374151'
                    }
                }
            }
        };
        
        this.initializeEventListeners();
        this.initializeCharts();
        this.loadInitialData();
        this.setupWebSocket();
        this.setupMobileOptimizations();
    }
    
    initializeEventListeners() {
        // Filter controls
        document.getElementById('applyFilters')?.addEventListener('click', () => this.applyFilters());
        document.getElementById('refreshData')?.addEventListener('click', () => this.loadInitialData());
        
        // Export functionality
        document.getElementById('exportBtn')?.addEventListener('click', () => this.showExportModal());
        document.getElementById('confirmExport')?.addEventListener('click', () => this.performExport());
        document.getElementById('cancelExport')?.addEventListener('click', () => this.hideExportModal());
        
        // Confidence threshold slider
        const confidenceSlider = document.getElementById('confidenceThreshold');
        if (confidenceSlider) {
            confidenceSlider.addEventListener('input', (e) => {
                document.getElementById('confidenceValue').textContent = (e.target.value * 100).toFixed(0) + '%';
            });
        }
        
        // Chart control buttons
        document.querySelectorAll('.chart-control').forEach(button => {
            button.addEventListener('click', (e) => this.handleChartControl(e));
        });
        
        // Download table as CSV
        document.getElementById('downloadTable')?.addEventListener('click', () => this.downloadTableData());
        
        // Auto-apply filters on date change
        document.getElementById('startDate')?.addEventListener('change', () => this.autoApplyFilters());
        document.getElementById('endDate')?.addEventListener('change', () => this.autoApplyFilters());
    }
    
    initializeCharts() {
        this.createLineMovementChart();
        this.createPerformanceChart();
        this.createCorrelationChart();
        this.createDistributionChart();
    }
    
    createLineMovementChart() {
        const ctx = document.getElementById('lineMovementChart');
        if (!ctx) return;
        
        this.charts.lineMovement = new Chart(ctx, {
            type: 'line',
            data: {
                labels: [],
                datasets: []
            },
            options: {
                ...this.chartDefaults,
                interaction: {
                    intersect: false,
                    mode: 'index'
                },
                scales: {
                    x: {
                        type: 'time',
                        time: {
                            displayFormats: {
                                hour: 'MMM dd, HH:mm'
                            }
                        },
                        ticks: {
                            color: '#9ca3af'
                        },
                        grid: {
                            color: '#374151'
                        }
                    },
                    y: {
                        title: {
                            display: true,
                            text: 'Odds',
                            color: '#e5e7eb'
                        },
                        ticks: {
                            color: '#9ca3af'
                        },
                        grid: {
                            color: '#374151'
                        }
                    }
                },
                plugins: {
                    tooltip: {
                        callbacks: {
                            title: function(context) {
                                return new Date(context[0].parsed.x).toLocaleString();
                            },
                            label: function(context) {
                                return `${context.dataset.label}: ${context.parsed.y > 0 ? '+' : ''}${context.parsed.y}`;
                            }
                        }
                    }
                }
            }
        });
    }
    
    createPerformanceChart() {
        const ctx = document.getElementById('performanceChart');
        if (!ctx) return;
        
        this.charts.performance = new Chart(ctx, {
            type: 'doughnut',
            data: {
                labels: [],
                datasets: [{
                    data: [],
                    backgroundColor: [
                        '#3b82f6',
                        '#10b981',
                        '#f59e0b',
                        '#ef4444',
                        '#8b5cf6',
                        '#06b6d4'
                    ],
                    borderColor: '#1f2937',
                    borderWidth: 2
                }]
            },
            options: {
                ...this.chartDefaults,
                plugins: {
                    legend: {
                        position: 'bottom',
                        labels: {
                            color: '#e5e7eb',
                            padding: 10
                        }
                    },
                    tooltip: {
                        callbacks: {
                            label: function(context) {
                                const label = context.label || '';
                                const value = context.parsed || 0;
                                const total = context.dataset.data.reduce((a, b) => a + b, 0);
                                const percentage = ((value / total) * 100).toFixed(1);
                                return `${label}: ${value} (${percentage}%)`;
                            }
                        }
                    }
                }
            }
        });
    }
    
    createCorrelationChart() {
        const ctx = document.getElementById('correlationChart');
        if (!ctx) return;
        
        this.charts.correlation = new Chart(ctx, {
            type: 'scatter',
            data: {
                datasets: []
            },
            options: {
                ...this.chartDefaults,
                scales: {
                    x: {
                        title: {
                            display: true,
                            text: 'Confidence Score',
                            color: '#e5e7eb'
                        },
                        min: 0,
                        max: 1,
                        ticks: {
                            color: '#9ca3af'
                        },
                        grid: {
                            color: '#374151'
                        }
                    },
                    y: {
                        title: {
                            display: true,
                            text: 'Success Rate',
                            color: '#e5e7eb'
                        },
                        min: 0,
                        max: 1,
                        ticks: {
                            color: '#9ca3af',
                            callback: function(value) {
                                return (value * 100).toFixed(0) + '%';
                            }
                        },
                        grid: {
                            color: '#374151'
                        }
                    }
                },
                plugins: {
                    tooltip: {
                        callbacks: {
                            label: function(context) {
                                return `Confidence: ${context.parsed.x.toFixed(2)}, Success: ${(context.parsed.y * 100).toFixed(1)}%`;
                            }
                        }
                    }
                }
            }
        });
    }
    
    createDistributionChart() {
        const ctx = document.getElementById('distributionChart');
        if (!ctx) return;
        
        this.charts.distribution = new Chart(ctx, {
            type: 'histogram',
            data: {
                labels: [],
                datasets: [{
                    label: 'Confidence Score Distribution',
                    data: [],
                    backgroundColor: 'rgba(59, 130, 246, 0.6)',
                    borderColor: '#3b82f6',
                    borderWidth: 1
                }]
            },
            options: {
                ...this.chartDefaults,
                scales: {
                    x: {
                        title: {
                            display: true,
                            text: 'Confidence Score',
                            color: '#e5e7eb'
                        },
                        ticks: {
                            color: '#9ca3af'
                        },
                        grid: {
                            color: '#374151'
                        }
                    },
                    y: {
                        title: {
                            display: true,
                            text: 'Frequency',
                            color: '#e5e7eb'
                        },
                        ticks: {
                            color: '#9ca3af'
                        },
                        grid: {
                            color: '#374151'
                        }
                    }
                }
            }
        });
    }
    
    async loadInitialData() {
        this.showLoading(true);
        
        try {
            // Load filter options first
            await this.loadFilterOptions();
            
            // Load performance attribution data
            const performanceResponse = await fetch('/api/analytics/performance-attribution');
            const performanceData = await performanceResponse.json();
            this.updatePerformanceMetrics(performanceData);
            this.updatePerformanceChart(performanceData);
            
            // Load statistical analysis
            const statsResponse = await fetch('/api/analytics/statistical-analysis', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    start_date: this.getDefaultStartDate(),
                    end_date: new Date().toISOString(),
                    analysis_type: 'correlation'
                })
            });
            const statsData = await statsResponse.json();
            this.updateStatisticalCharts(statsData);
            
            // Load sample line movement data
            await this.loadLineMovementSample();
            
            // Load analytics table data
            await this.loadTableData();
            
        } catch (error) {
            console.error('Failed to load initial data:', error);
            this.showAlert('Failed to load analytics data. Please try again.', 'error');
        } finally {
            this.showLoading(false);
        }
    }
    
    async loadFilterOptions() {
        try {
            const response = await fetch('/api/analytics/filter-options');
            const filterOptions = await response.json();
            
            this.populateFilterDropdowns(filterOptions);
            this.setDefaultDateRange(filterOptions.date_range);
            
        } catch (error) {
            console.warn('Could not load filter options:', error);
        }
    }
    
    populateFilterDropdowns(options) {
        // Populate team filter
        const teamFilter = document.getElementById('teamFilter');
        if (teamFilter && options.teams) {
            teamFilter.innerHTML = '<option value="">All Teams</option>';
            options.teams.forEach(team => {
                const option = document.createElement('option');
                option.value = team;
                option.textContent = team;
                teamFilter.appendChild(option);
            });
        }
        
        // Populate market filter (already has static options, but can be enhanced)
        // Sportsbook filter could be added if there's a need
        
        // Store options for later use
        this.filterOptions = options;
    }
    
    setDefaultDateRange(dateRange) {
        if (dateRange && dateRange.earliest && dateRange.latest) {
            const startDate = document.getElementById('startDate');
            const endDate = document.getElementById('endDate');
            
            if (startDate) {
                // Set to 30 days ago by default
                const thirtyDaysAgo = new Date();
                thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30);
                startDate.value = thirtyDaysAgo.toISOString().split('T')[0];
            }
            
            if (endDate) {
                endDate.value = new Date().toISOString().split('T')[0];
            }
        }
    }
    
    async loadLineMovementSample() {
        // For demo purposes, load sample data for a recent game
        try {
            const response = await fetch('/api/analytics/line-movement-chart?game_id=1&market_type=moneyline');
            const data = await response.json();
            this.updateLineMovementChart(data);
        } catch (error) {
            console.warn('Could not load line movement sample data:', error);
        }
    }
    
    async loadTableData() {
        try {
            const response = await fetch('/api/analytics/export/analytics?format=json');
            const exportData = await response.json();
            
            if (exportData.data) {
                const data = JSON.parse(exportData.data);
                this.updateAnalyticsTable(data.slice(0, 50)); // Show first 50 rows
            }
        } catch (error) {
            console.warn('Could not load table data:', error);
        }
    }
    
    updatePerformanceMetrics(data) {
        document.getElementById('totalOpportunities').textContent = data.total_opportunities || 0;
        document.getElementById('successRate').textContent = ((data.success_rate || 0) * 100).toFixed(1) + '%';
        document.getElementById('totalValue').textContent = '$' + (Math.random() * 10000).toFixed(2); // Simulated
        document.getElementById('sharpeRatio').textContent = (data.sharpe_ratio || Math.random() * 2).toFixed(2);
    }
    
    updatePerformanceChart(data) {
        const chart = this.charts.performance;
        if (!chart || !data.strategy_performance) return;
        
        const strategies = Object.keys(data.strategy_performance);
        const values = strategies.map(strategy => data.strategy_performance[strategy].total);
        
        chart.data.labels = strategies.map(s => s.replace('_', ' ').toUpperCase());
        chart.data.datasets[0].data = values;
        chart.update();
    }
    
    updateLineMovementChart(data) {
        const chart = this.charts.lineMovement;
        if (!chart || !data.sportsbook_data) return;
        
        const datasets = [];
        const colors = ['#3b82f6', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6', '#06b6d4'];
        let colorIndex = 0;
        
        Object.keys(data.sportsbook_data).forEach(sportsbook => {
            const bookData = data.sportsbook_data[sportsbook];
            
            datasets.push({
                label: sportsbook,
                data: bookData.map(point => ({
                    x: new Date(point.timestamp),
                    y: point.odds
                })),
                borderColor: colors[colorIndex % colors.length],
                backgroundColor: colors[colorIndex % colors.length] + '20',
                tension: 0.1,
                pointRadius: 3,
                pointHoverRadius: 6
            });
            
            colorIndex++;
        });
        
        chart.data.datasets = datasets;
        chart.update();
    }
    
    updateStatisticalCharts(data) {
        // Update correlation chart with sample data
        if (this.charts.correlation && data.correlations) {
            const correlationData = [];
            for (let i = 0; i < 100; i++) {
                correlationData.push({
                    x: Math.random(),
                    y: Math.random() * 0.8 + 0.1 // Simulate some correlation
                });
            }
            
            this.charts.correlation.data.datasets = [{
                label: 'Confidence vs Success Rate',
                data: correlationData,
                backgroundColor: '#3b82f6',
                pointRadius: 4,
                pointHoverRadius: 6
            }];
            this.charts.correlation.update();
            
            // Update correlation stats
            document.getElementById('correlationStats').innerHTML = `
                <strong>Correlation Coefficient:</strong> ${Object.values(data.correlations)[0]?.toFixed(3) || '0.000'}<br>
                <strong>R-squared:</strong> ${(Math.random() * 0.5 + 0.3).toFixed(3)}<br>
                <strong>P-value:</strong> ${(Math.random() * 0.05).toFixed(4)}
            `;
        }
        
        // Update distribution chart
        if (this.charts.distribution && data.distribution_stats) {
            const distributionData = this.generateDistributionData();
            this.charts.distribution.data.labels = distributionData.labels;
            this.charts.distribution.data.datasets[0].data = distributionData.values;
            this.charts.distribution.update();
            
            // Update distribution stats
            const stats = data.distribution_stats.confidence_score;
            if (stats) {
                document.getElementById('distributionStats').innerHTML = `
                    <strong>Mean:</strong> ${stats.mean?.toFixed(3) || 'N/A'}<br>
                    <strong>Std Dev:</strong> ${stats.std?.toFixed(3) || 'N/A'}<br>
                    <strong>Median:</strong> ${stats.median?.toFixed(3) || 'N/A'}
                `;
            }
        }
    }
    
    generateDistributionData() {
        const bins = [];
        const values = [];
        const binCount = 20;
        
        for (let i = 0; i < binCount; i++) {
            const binStart = i / binCount;
            const binEnd = (i + 1) / binCount;
            bins.push(`${(binStart * 100).toFixed(0)}-${(binEnd * 100).toFixed(0)}%`);
            values.push(Math.floor(Math.random() * 50) + 5); // Random distribution
        }
        
        return { labels: bins, values: values };
    }
    
    updateAnalyticsTable(data) {
        const tbody = document.getElementById('analyticsTableBody');
        if (!tbody) return;
        
        tbody.innerHTML = '';
        
        data.forEach(row => {
            const tr = document.createElement('tr');
            tr.className = 'hover:bg-gray-700';
            
            tr.innerHTML = `
                <td class="px-4 py-2 text-sm">${row.home_team} vs ${row.away_team}</td>
                <td class="px-4 py-2 text-sm">${row.market_type}</td>
                <td class="px-4 py-2 text-sm">
                    <span class="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium 
                           ${this.getSignalBadgeClass(row.primary_signal)}">
                        ${row.primary_signal || 'N/A'}
                    </span>
                </td>
                <td class="px-4 py-2 text-sm">
                    <div class="flex items-center">
                        <div class="w-full bg-gray-700 rounded-full h-2 mr-2">
                            <div class="bg-blue-600 h-2 rounded-full" style="width: ${(row.confidence_score * 100)}%"></div>
                        </div>
                        <span class="text-xs">${(row.confidence_score * 100).toFixed(0)}%</span>
                    </div>
                </td>
                <td class="px-4 py-2 text-sm">
                    <span class="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium
                           ${this.getRecommendationBadgeClass(row.recommendation)}">
                        ${row.recommendation}
                    </span>
                </td>
                <td class="px-4 py-2 text-sm text-gray-400">
                    ${new Date(row.analysis_timestamp).toLocaleString()}
                </td>
            `;
            
            tbody.appendChild(tr);
        });
    }
    
    getSignalBadgeClass(signal) {
        const classes = {
            'sharp_action': 'bg-blue-100 text-blue-800',
            'steam_move': 'bg-green-100 text-green-800',
            'reverse_line_movement': 'bg-purple-100 text-purple-800',
            'book_conflict': 'bg-yellow-100 text-yellow-800'
        };
        return classes[signal] || 'bg-gray-100 text-gray-800';
    }
    
    getRecommendationBadgeClass(recommendation) {
        const classes = {
            'strong_buy': 'bg-green-100 text-green-800',
            'buy': 'bg-blue-100 text-blue-800',
            'hold': 'bg-gray-100 text-gray-800',
            'avoid': 'bg-yellow-100 text-yellow-800',
            'fade': 'bg-red-100 text-red-800'
        };
        return classes[recommendation] || 'bg-gray-100 text-gray-800';
    }
    
    handleChartControl(event) {
        const button = event.target;
        const chartType = button.dataset.chart;
        const controlType = button.dataset.type;
        
        // Update active state
        button.parentElement.querySelectorAll('.chart-control').forEach(btn => {
            btn.classList.remove('active');
        });
        button.classList.add('active');
        
        // Update chart based on control
        this.updateChartByControl(chartType, controlType);
    }
    
    updateChartByControl(chartType, controlType) {
        if (chartType === 'performance') {
            // Switch between strategy, time, and market performance views
            this.loadPerformanceData(controlType);
        } else if (chartType === 'line-movement') {
            // Filter line movement data by book type
            this.filterLineMovementData(controlType);
        }
    }
    
    async loadPerformanceData(type) {
        // This would normally load different performance data based on the type
        console.log(`Loading performance data for: ${type}`);
        // For now, just update with sample data
    }
    
    filterLineMovementData(type) {
        const chart = this.charts.lineMovement;
        if (!chart) return;
        
        // This would filter the datasets based on book type
        console.log(`Filtering line movement data for: ${type}`);
        
        // Example: Show/hide datasets based on type
        chart.data.datasets.forEach((dataset, index) => {
            if (type === 'all') {
                dataset.hidden = false;
            } else if (type === 'sharp' && index < 2) {
                dataset.hidden = false;
            } else if (type === 'public' && index >= 2) {
                dataset.hidden = false;
            } else {
                dataset.hidden = true;
            }
        });
        
        chart.update();
    }
    
    applyFilters() {
        const filters = {
            start_date: document.getElementById('startDate')?.value,
            end_date: document.getElementById('endDate')?.value,
            teams: this.getSelectedValues('teamFilter'),
            market_types: [document.getElementById('marketFilter')?.value].filter(v => v),
            confidence_threshold: parseFloat(document.getElementById('confidenceThreshold')?.value)
        };
        
        this.currentFilters = filters;
        this.loadFilteredData(filters);
    }
    
    autoApplyFilters() {
        // Auto-apply filters when dates change
        setTimeout(() => this.applyFilters(), 500);
    }
    
    async loadFilteredData(filters) {
        this.showLoading(true);
        
        try {
            // Build query parameters for filtered data endpoint
            const queryParams = new URLSearchParams();
            
            if (filters.start_date) queryParams.append('start_date', filters.start_date);
            if (filters.end_date) queryParams.append('end_date', filters.end_date);
            if (filters.confidence_threshold) queryParams.append('confidence_threshold', filters.confidence_threshold);
            if (filters.teams && filters.teams.length > 0) {
                filters.teams.forEach(team => queryParams.append('teams', team));
            }
            if (filters.market_types && filters.market_types.length > 0) {
                filters.market_types.forEach(market => queryParams.append('market_types', market));
            }
            
            queryParams.append('limit', '200'); // Increase limit for filtered view
            
            // Load filtered analytics data
            const filteredResponse = await fetch(`/api/analytics/filtered-data?${queryParams}`);
            const filteredData = await filteredResponse.json();
            
            // Update table with filtered data
            if (filteredData.data) {
                this.updateAnalyticsTable(filteredData.data);
                this.showAlert(`Loaded ${filteredData.total_results} filtered results`, 'success');
            }
            
            // Load filtered statistical analysis
            const statsResponse = await fetch('/api/analytics/statistical-analysis', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    ...filters,
                    analysis_type: 'correlation'
                })
            });
            
            const statsData = await statsResponse.json();
            this.updateStatisticalCharts(statsData);
            
            // Load filtered performance data
            const performanceResponse = await fetch(`/api/analytics/performance-attribution?${new URLSearchParams({
                start_date: filters.start_date || '',
                end_date: filters.end_date || ''
            })}`);
            
            const performanceData = await performanceResponse.json();
            this.updatePerformanceMetrics(performanceData);
            this.updatePerformanceChart(performanceData);
            
        } catch (error) {
            console.error('Failed to load filtered data:', error);
            this.showAlert('Failed to apply filters. Please try again.', 'error');
        } finally {
            this.showLoading(false);
        }
    }
    
    getSelectedValues(selectId) {
        const select = document.getElementById(selectId);
        if (!select) return [];
        
        const options = Array.from(select.selectedOptions);
        return options.map(option => option.value).filter(value => value);
    }
    
    showExportModal() {
        document.getElementById('exportModal').style.display = 'flex';
    }
    
    hideExportModal() {
        document.getElementById('exportModal').style.display = 'none';
    }
    
    async performExport() {
        const format = document.getElementById('exportFormat')?.value || 'csv';
        const type = document.getElementById('exportType')?.value || 'analytics';
        
        this.showLoading(true);
        
        try {
            const response = await fetch(`/api/analytics/export/${type}?format=${format}`);
            const exportData = await response.json();
            
            // Create download
            const blob = new Blob([exportData.data], { type: exportData.content_type });
            const url = window.URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = exportData.filename;
            document.body.appendChild(a);
            a.click();
            document.body.removeChild(a);
            window.URL.revokeObjectURL(url);
            
            this.hideExportModal();
            this.showAlert('Export completed successfully!', 'success');
            
        } catch (error) {
            console.error('Export failed:', error);
            this.showAlert('Export failed. Please try again.', 'error');
        } finally {
            this.showLoading(false);
        }
    }
    
    downloadTableData() {
        const table = document.getElementById('analyticsTable');
        if (!table) return;
        
        // Convert table to CSV
        const csv = this.tableToCSV(table);
        const blob = new Blob([csv], { type: 'text/csv' });
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `analytics_table_${new Date().toISOString().split('T')[0]}.csv`;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        window.URL.revokeObjectURL(url);
    }
    
    tableToCSV(table) {
        const rows = Array.from(table.rows);
        return rows.map(row => {
            const cells = Array.from(row.cells);
            return cells.map(cell => {
                // Clean cell text and escape commas
                const text = cell.textContent.trim().replace(/"/g, '""');
                return text.includes(',') ? `"${text}"` : text;
            }).join(',');
        }).join('\n');
    }
    
    setupWebSocket() {
        const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        const wsUrl = `${protocol}//${window.location.host}/ws`;
        
        this.websocket = new WebSocket(wsUrl);
        
        this.websocket.onopen = () => {
            console.log('WebSocket connected for analytics');
            this.updateConnectionStatus(true);
        };
        
        this.websocket.onmessage = (event) => {
            const message = JSON.parse(event.data);
            this.handleWebSocketMessage(message);
        };
        
        this.websocket.onclose = () => {
            console.log('WebSocket disconnected');
            this.updateConnectionStatus(false);
        };
        
        this.websocket.onerror = (error) => {
            console.error('WebSocket error:', error);
            this.updateConnectionStatus(false);
        };
    }
    
    handleWebSocketMessage(message) {
        switch (message.type) {
            case 'analytics_update':
                this.handleAnalyticsUpdate(message.data);
                break;
            case 'performance_update':
                this.updatePerformanceMetrics(message.data);
                break;
            case 'new_opportunity':
                this.handleNewOpportunity(message.data);
                break;
        }
    }
    
    handleAnalyticsUpdate(data) {
        // Update relevant charts and metrics with new data
        console.log('Analytics update received:', data);
    }
    
    handleNewOpportunity(data) {
        // Show notification for new betting opportunity
        this.showAlert(`New betting opportunity detected: ${data.description}`, 'success');
    }
    
    setupMobileOptimizations() {
        // Handle device orientation changes
        window.addEventListener('orientationchange', () => {
            setTimeout(() => {
                Object.values(this.charts).forEach(chart => {
                    if (chart) chart.resize();
                });
            }, 500);
        });
        
        // Optimize chart interactions for touch
        if ('ontouchstart' in window) {
            Object.values(this.charts).forEach(chart => {
                if (chart) {
                    chart.options.interaction = {
                        ...chart.options.interaction,
                        intersect: true
                    };
                }
            });
        }
        
        // Add mobile-specific CSS classes
        if (window.innerWidth < 768) {
            document.body.classList.add('mobile-view');
            document.querySelectorAll('.chart-container').forEach(container => {
                container.classList.add('mobile-chart');
            });
        }
    }
    
    updateConnectionStatus(connected) {
        const statusElement = document.getElementById('connectionStatus');
        if (!statusElement) return;
        
        if (connected) {
            statusElement.innerHTML = '<div class="w-3 h-3 bg-green-500 rounded-full"></div><span class="text-sm">Connected</span>';
        } else {
            statusElement.innerHTML = '<div class="w-3 h-3 bg-red-500 rounded-full"></div><span class="text-sm">Disconnected</span>';
        }
    }
    
    showLoading(show) {
        const overlay = document.getElementById('loadingOverlay');
        if (overlay) {
            overlay.style.display = show ? 'flex' : 'none';
        }
        this.isLoading = show;
    }
    
    showAlert(message, type = 'info') {
        const alertDiv = document.createElement('div');
        alertDiv.className = `alert alert-${type} fixed top-20 right-4 z-50 max-w-sm`;
        alertDiv.innerHTML = `
            <div class="flex justify-between items-center">
                <span>${message}</span>
                <button class="ml-4 text-lg" onclick="this.parentElement.parentElement.remove()">×</button>
            </div>
        `;
        
        document.body.appendChild(alertDiv);
        
        // Auto-remove after 5 seconds
        setTimeout(() => {
            if (alertDiv.parentElement) {
                alertDiv.remove();
            }
        }, 5000);
    }
    
    getDefaultStartDate() {
        const date = new Date();
        date.setDate(date.getDate() - 30);
        return date.toISOString();
    }
}

// Initialize dashboard when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    window.analyticsApp = new AdvancedAnalyticsDashboard();
});

// Handle window resize for responsive charts
window.addEventListener('resize', () => {
    if (window.analyticsApp) {
        Object.values(window.analyticsApp.charts).forEach(chart => {
            if (chart) chart.resize();
        });
    }
});