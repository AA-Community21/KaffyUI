// Kaffy UI JavaScript
class KaffyUI {
    constructor() {
        this.baseUrl = '/api';
        this.currentView = 'dashboard';
        this.init();
    }

    init() {
        this.setupNavigation();
        this.handleInitialRoute();
        this.startClusterInfoUpdates();
    }

    handleInitialRoute() {
        const path = window.location.pathname;
        
        if (path === '/') {
            this.showIntroVideo();
        } else if (path.startsWith('/topic/')) {
            const topicName = path.split('/')[2];
            this.skipVideoAndLoadTopic(topicName);
        } else if (path.startsWith('/messages/')) {
            const parts = path.split('/');
            const topicName = parts[2];
            const partition = parseInt(parts[3]);
            this.skipVideoAndLoadMessages(topicName, partition);
        } else if (path.startsWith('/message/')) {
            const parts = path.split('/');
            const topicName = parts[2];
            const partition = parseInt(parts[3]);
            const offset = parseInt(parts[4]);
            this.skipVideoAndLoadMessage(topicName, partition, offset);
        } else if (path === '/create-topic') {
            this.skipVideoAndLoadCreateTopic();
        } else {
            this.showIntroVideo();
        }
    }

    skipVideoAndLoadTopic(topicName) {
        const introDiv = document.getElementById('intro-video');
        introDiv.style.display = 'none';
        this.loadTopicDetails(topicName);
    }

    skipVideoAndLoadMessages(topicName, partition) {
        const introDiv = document.getElementById('intro-video');
        introDiv.style.display = 'none';
        this.loadPartitionMessagesPage(topicName, partition);
    }

    skipVideoAndLoadMessage(topicName, partition, offset) {
        const introDiv = document.getElementById('intro-video');
        introDiv.style.display = 'none';
        this.loadMessageDetails(topicName, partition, offset);
    }

    skipVideoAndLoadCreateTopic() {
        const introDiv = document.getElementById('intro-video');
        introDiv.style.display = 'none';
        this.loadCreateTopic();
    }

    startClusterInfoUpdates() {
        // Update cluster info every 30 seconds
        setInterval(async () => {
            if (document.getElementById('main-content').innerHTML.includes('Cluster Information')) {
                try {
                    const clusterInfo = await this.fetchData('/clusterInfo');
                    this.updateClusterInfoDisplay(clusterInfo);
                } catch (error) {
                    console.error('Failed to update cluster info:', error);
                }
            }
        }, 30000);
    }

    updateClusterInfoDisplay(clusterInfo) {
        // Update stat cards
        const statCards = document.querySelectorAll('.stat-card');
        if (statCards.length >= 4) {
            statCards[0].querySelector('.stat-value').textContent = clusterInfo.totalTopics;
            statCards[1].querySelector('.stat-value').textContent = clusterInfo.totalPartitions;
            statCards[2].querySelector('.stat-value').textContent = clusterInfo.totalBrokers;
            statCards[3].querySelector('.stat-value').textContent = clusterInfo.preferredLeaderPercentage.toFixed(1) + '%';
        }
        
        // Update cluster info table
        const clusterTable = document.querySelector('.card-body table');
        if (clusterTable) {
            const rows = clusterTable.querySelectorAll('tr');
            if (rows.length >= 4) {
                rows[3].querySelector('td:last-child').textContent = clusterInfo.underReplicatedPartitions;
            }
        }
    }

    showIntroVideo() {
        const video = document.querySelector('#intro-video video');
        const introDiv = document.getElementById('intro-video');
        
        // Start loading dashboard in background
        this.loadDashboardInBackground();
        
        video.addEventListener('ended', () => {
            this.skipVideo();
        });
        
        // Fallback if video doesn't load
        setTimeout(() => {
            if (introDiv.style.display !== 'none') {
                this.skipVideo();
            }
        }, 10000);
    }

    async loadDashboardInBackground() {
        try {
            // Load critical data first
            const clusterInfo = await this.fetchData('/clusterInfo');
            
            // Load remaining data in parallel
            const [topics, brokers] = await Promise.all([
                this.fetchData('/topics'),
                this.fetchData('/brokerInfo')
            ]);
            
            this.dashboardData = { clusterInfo, topics, brokers };
            
            // Cache all topic details and broker details
            await this.cacheAllTopicDetails(topics);
            this.cachedBrokers = brokers;
            
        } catch (error) {
            this.dashboardError = 'Failed to load dashboard data';
        }
    }

    async cacheAllTopicDetails(topics) {
        this.topicCache = {};
        const topicPromises = topics.map(async (topic) => {
            try {
                const details = await this.fetchData(`/topics/${topic.topicName}`);
                this.topicCache[topic.topicName] = {
                    data: details,
                    timestamp: Date.now()
                };
            } catch (error) {
                console.error(`Failed to cache topic ${topic.topicName}:`, error);
            }
        });
        
        await Promise.all(topicPromises);
    }

    skipVideo() {
        const introDiv = document.getElementById('intro-video');
        introDiv.classList.add('fade-out');
        setTimeout(() => {
            introDiv.style.display = 'none';
            // Only show dashboard if we're on the root path
            if (window.location.pathname === '/') {
                this.showDashboard();
            }
        }, 1000);
    }

    showDashboard() {
        const content = document.getElementById('main-content');
        
        if (this.dashboardError) {
            content.innerHTML = this.getErrorHTML(this.dashboardError);
        } else if (this.dashboardData) {
            content.innerHTML = this.getDashboardHTML(
                this.dashboardData.clusterInfo, 
                this.dashboardData.topics, 
                this.dashboardData.brokers
            ) + this.getCreateTopicModalHTML();
            content.classList.add('dashboard-entrance');
            this.setupDashboardSearch();
        } else {
            content.innerHTML = this.getLoadingHTML();
            setTimeout(() => this.loadDashboard(), 500);
        }
    }

    setupNavigation() {
        // Navigation removed - only dashboard and topic details
    }

    async loadDashboard() {
        // Change URL to root
        window.history.pushState({view: 'dashboard'}, '', '/');
        
        const content = document.getElementById('main-content');
        content.innerHTML = this.getLoadingHTML();

        try {
            const [clusterInfo, topics, brokers] = await Promise.all([
                this.fetchData('/clusterInfo'),
                this.fetchData('/topics'),
                this.fetchData('/brokerInfo')
            ]);

            content.innerHTML = this.getDashboardHTML(clusterInfo, topics, brokers) + this.getCreateTopicModalHTML();
            content.classList.add('dashboard-entrance');
            this.setupDashboardSearch();
        } catch (error) {
            content.innerHTML = this.getErrorHTML('Failed to load dashboard data');
        }
    }

    async loadTopics() {
        const content = document.getElementById('main-content');
        content.innerHTML = this.getLoadingHTML();

        try {
            const topics = await this.fetchData('/topics');
            content.innerHTML = this.getTopicsHTML(topics);
            this.setupTopicSearch();
        } catch (error) {
            content.innerHTML = this.getErrorHTML('Failed to load topics');
        }
    }

    async loadBrokers() {
        const content = document.getElementById('main-content');
        
        // Check if we have cached brokers
        if (this.cachedBrokers) {
            content.innerHTML = this.getBrokersHTML(this.cachedBrokers);
            return;
        }
        
        content.innerHTML = this.getLoadingHTML();

        try {
            const brokers = await this.fetchData('/brokerInfo');
            this.cachedBrokers = brokers;
            content.innerHTML = this.getBrokersHTML(brokers);
        } catch (error) {
            content.innerHTML = this.getErrorHTML('Failed to load brokers');
        }
    }

    async loadProducer() {
        const content = document.getElementById('main-content');
        content.innerHTML = this.getLoadingHTML();
        
        const producerHTML = await this.getProducerHTML();
        content.innerHTML = producerHTML;
        this.setupProducerForm();
    }

    async loadTopicDetails(topicName) {
        // Change URL without page reload
        window.history.pushState({view: 'topic', topicName: topicName}, '', `/topic/${topicName}`);
        
        const content = document.getElementById('main-content');
        
        // Check if we have cached data
        const cached = this.topicCache && this.topicCache[topicName];
        const cacheAge = cached ? Date.now() - cached.timestamp : Infinity;
        const cacheExpiry = 60000; // 1 minute
        
        if (cached && cacheAge < cacheExpiry) {
            // Use cached data
            content.innerHTML = this.getTopicDetailsHTML(cached.data);
            this.setupPartitionViewer(topicName);
            return;
        }
        
        // Show loading for fresh data
        content.innerHTML = `
            <div class="card">
                <div class="card-header">
                    Topic: ${topicName}
                    <button class="btn btn-primary" style="float: right;" onclick="kaffyUI.loadDashboard()">Back to Dashboard</button>
                </div>
                <div class="card-body">
                    <div class="loading">
                        <div class="spinner"></div>
                        <p>Loading topic details...</p>
                    </div>
                </div>
            </div>
        `;

        try {
            const topicDetails = await this.fetchData(`/topics/${topicName}`);
            
            // Update cache
            if (!this.topicCache) this.topicCache = {};
            this.topicCache[topicName] = {
                data: topicDetails,
                timestamp: Date.now()
            };
            
            content.innerHTML = this.getTopicDetailsHTML(topicDetails);
            this.setupPartitionViewer(topicName);
        } catch (error) {
            content.innerHTML = this.getErrorHTML(`Failed to load topic details for ${topicName}`);
        }
    }

    async fetchData(endpoint) {
        try {
            const response = await fetch(this.baseUrl + endpoint, {
                headers: {
                    'Cache-Control': 'no-cache',
                    'Accept': 'application/json'
                }
            });
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            return await response.json();
        } catch (error) {
            console.error('Fetch error:', error);
            throw error;
        }
    }

    showNotification(message, type = 'info') {
        const notification = document.createElement('div');
        notification.className = `notification notification-${type}`;
        notification.innerHTML = `
            <span>${message}</span>
            <button onclick="this.parentElement.remove()">&times;</button>
        `;
        notification.style.cssText = `
            position: fixed;
            top: 20px;
            right: 20px;
            padding: 1rem 1.5rem;
            border-radius: 4px;
            color: white;
            z-index: 1001;
            display: flex;
            align-items: center;
            gap: 1rem;
            max-width: 400px;
            box-shadow: 0 4px 12px rgba(0,0,0,0.3);
            background: ${type === 'success' ? '#27ae60' : type === 'error' ? '#e74c3c' : '#3498db'};
        `;
        document.body.appendChild(notification);
        setTimeout(() => notification.remove(), 5000);
    }

    getDashboardHTML(clusterInfo, topics, brokers) {
        return `
            <div class="stats-grid">
                <div class="stat-card">
                    <div class="stat-value">${clusterInfo.totalTopics}</div>
                    <div class="stat-label">Total Topics</div>
                </div>
                <div class="stat-card">
                    <div class="stat-value">${clusterInfo.totalPartitions}</div>
                    <div class="stat-label">Total Partitions</div>
                </div>
                <div class="stat-card">
                    <div class="stat-value">${brokers.length}</div>
                    <div class="stat-label">Brokers</div>
                </div>
                <div class="stat-card">
                    <div class="stat-value">${clusterInfo.preferredLeaderPercentage.toFixed(1)}%</div>
                    <div class="stat-label">Preferred Leader %</div>
                </div>
            </div>

            <div class="card">
                <div class="card-header">Cluster Information</div>
                <div class="card-body">
                    <table>
                        <tr><td><strong>Cluster ID:</strong></td><td>${clusterInfo.clusterId}</td></tr>
                        <tr><td><strong>Bootstrap Servers:</strong></td><td>${clusterInfo.bootstrapServers}</td></tr>
                        <tr><td><strong>Controller:</strong></td><td>Broker ${clusterInfo.controller.id} (${clusterInfo.controller.host}:${clusterInfo.controller.port})</td></tr>
                        <tr><td><strong>Under Replicated Partitions:</strong></td><td>${clusterInfo.underReplicatedPartitions}</td></tr>
                    </table>
                </div>
            </div>

            <div class="card">
                <div class="card-header">
                    Controller Broker
                    <button class="btn btn-primary" style="float: right;" onclick="kaffyUI.loadBrokers()">View All Brokers</button>
                </div>
                <div class="card-body">
                    <div class="table-container">
                        <table>
                            <thead>
                                <tr>
                                    <th>Broker ID</th>
                                    <th>Host</th>
                                    <th>Port</th>
                                    <th>Partitions</th>
                                    <th>Partition %</th>
                                </tr>
                            </thead>
                            <tbody>
                                ${brokers.filter(broker => broker.controller).map(broker => `
                                    <tr>
                                        <td>${broker.brokerId}</td>
                                        <td>${broker.hostname}</td>
                                        <td>${broker.port}</td>
                                        <td>${broker.partitionCount}</td>
                                        <td>${broker.partitionPercentage.toFixed(1)}%</td>
                                    </tr>
                                `).join('')}
                            </tbody>
                        </table>
                    </div>
                </div>
            </div>

            <div class="card">
                <div class="card-header">Topics</div>
                <div class="card-body">
                    <div class="search-container" style="display: flex; gap: 1rem; align-items: center; margin-bottom: 1.5rem;">
                        <input type="text" id="dashboard-topic-search" class="search-input" placeholder="Search topics..." style="flex: 1;">
                        <button class="btn btn-success" onclick="kaffyUI.loadCreateTopic()">Create Topic</button>
                    </div>
                    <div class="table-container">
                        <table>
                            <thead>
                                <tr>
                                    <th>Topic Name</th>
                                    <th>Partitions</th>
                                    <th>Preferred %</th>
                                    <th>Under Replicated</th>
                                    <th>Custom Config</th>
                                    <th>Actions</th>
                                </tr>
                            </thead>
                            <tbody>
                                ${topics.slice(0, 10).map(topic => `
                                    <tr style="cursor: pointer;">
                                        <td onclick="kaffyUI.loadTopicDetails('${topic.topicName}')">
                                            <span class="health-indicator ${this.getHealthClass(topic)}"></span>
                                            <strong>${topic.topicName}</strong>
                                        </td>
                                        <td>${topic.partitionCount}</td>
                                        <td>${topic.preferredPercentage.toFixed(1)}%</td>
                                        <td>${topic.underReplicatedPartitions}</td>
                                        <td>${topic.hasCustomConfig ? '<span class="badge badge-info">Yes</span>' : 'No'}</td>
                                        <td>
                                            <button class="btn btn-danger btn-sm" onclick="event.stopPropagation(); kaffyUI.deleteTopic('${topic.topicName}')">Delete</button>
                                        </td>
                                    </tr>
                                `).join('')}
                            </tbody>
                        </table>
                    </div>
                </div>
            </div>
        `;
    }

    getTopicsHTML(topics) {
        return `
            <div class="card">
                <div class="card-header">
                    Topics (${topics.length})
                    <button class="btn btn-primary" style="float: right;" onclick="kaffyUI.showCreateTopicModal()">Create Topic</button>
                </div>
                <div class="card-body">
                    <div class="search-container">
                        <input type="text" id="topic-search" class="search-input" placeholder="Search topics...">
                    </div>
                    <div class="table-container">
                        <table id="topics-table">
                            <thead>
                                <tr>
                                    <th>Topic Name</th>
                                    <th>Partitions</th>
                                    <th>Preferred %</th>
                                    <th>Under Replicated</th>
                                    <th>Custom Config</th>
                                    <th>Actions</th>
                                </tr>
                            </thead>
                            <tbody>
                                ${topics.map(topic => `
                                    <tr data-topic="${topic.topicName}">
                                        <td>
                                            <span class="health-indicator ${this.getHealthClass(topic)}"></span>
                                            <a href="#" onclick="kaffyUI.loadTopicDetails('${topic.topicName}')">${topic.topicName}</a>
                                        </td>
                                        <td>${topic.partitionCount}</td>
                                        <td>${topic.preferredPercentage.toFixed(1)}%</td>
                                        <td>${topic.underReplicatedPartitions}</td>
                                        <td>${topic.hasCustomConfig ? '<span class="badge badge-info">Yes</span>' : 'No'}</td>
                                        <td>
                                            <button class="btn btn-danger btn-sm" onclick="kaffyUI.deleteTopic('${topic.topicName}')">Delete</button>
                                        </td>
                                    </tr>
                                `).join('')}
                            </tbody>
                        </table>
                    </div>
                </div>
            </div>
            ${this.getCreateTopicModalHTML()}
        `;
    }

    getBrokersHTML(brokers) {
        return `
            <div class="card">
                <div class="card-header">
                    Brokers (${brokers.length})
                    <button class="btn btn-primary" style="float: right;" onclick="kaffyUI.loadDashboard()">Back to Dashboard</button>
                </div>
                <div class="card-body">
                    <div class="table-container">
                        <table>
                            <thead>
                                <tr>
                                    <th>Broker ID</th>
                                    <th>Host</th>
                                    <th>Port</th>
                                    <th>Rack</th>
                                    <th>Controller</th>
                                    <th>Partitions</th>
                                    <th>Partition %</th>
                                </tr>
                            </thead>
                            <tbody>
                                ${brokers.map(broker => `
                                    <tr>
                                        <td>${broker.brokerId}</td>
                                        <td>${broker.hostname}</td>
                                        <td>${broker.port}</td>
                                        <td>${broker.rack || 'N/A'}</td>
                                        <td>${broker.controller ? '<span class="badge badge-success">Yes</span>' : 'No'}</td>
                                        <td>${broker.partitionCount}</td>
                                        <td>${broker.partitionPercentage.toFixed(1)}%</td>
                                    </tr>
                                `).join('')}
                            </tbody>
                        </table>
                    </div>
                </div>
            </div>
        `;
    }

    async getProducerHTML() {
        try {
            const topics = await this.fetchData('/topics');
            const topicOptions = topics.map(topic => 
                `<option value="${topic.topicName}">${topic.topicName}</option>`
            ).join('');
            
            return `
                <div class="card">
                    <div class="card-header">
                        Message Producer
                        <button class="btn btn-primary" style="float: right;" onclick="kaffyUI.loadDashboard()">Back to Dashboard</button>
                    </div>
                    <div class="card-body">
                        <form id="producer-form">
                            <div class="form-group">
                                <label class="form-label">Topic Name</label>
                                <select id="producer-topic" class="form-control" required>
                                    <option value="">Select a topic...</option>
                                    ${topicOptions}
                                </select>
                            </div>
                            <div class="form-group">
                                <label class="form-label">Key (Optional)</label>
                                <input type="text" id="producer-key" class="form-control">
                            </div>
                            <div class="form-group">
                                <label class="form-label">Message Value</label>
                                <textarea id="producer-value" class="form-control" rows="5" required></textarea>
                            </div>
                            <button type="submit" class="btn btn-success">Send Message</button>
                        </form>
                    </div>
                </div>
            `;
        } catch (error) {
            return `
                <div class="card">
                    <div class="card-header">
                        Message Producer
                        <button class="btn btn-primary" style="float: right;" onclick="kaffyUI.loadDashboard()">Back to Dashboard</button>
                    </div>
                    <div class="card-body">
                        <p style="color: #e74c3c;">Failed to load topics. Please try again.</p>
                    </div>
                </div>
            `;
        }
    }

    getTopicDetailsHTML(topicDetails) {
        return `
            <div class="card">
                <div class="card-header">
                    Topic: ${topicDetails.topicName}
                    <button class="btn btn-primary" style="float: right;" onclick="kaffyUI.loadDashboard()">Back to Dashboard</button>
                </div>
                <div class="card-body">
                    <div class="stats-grid">
                        <div class="stat-card">
                            <div class="stat-value">${topicDetails.partitionCount}</div>
                            <div class="stat-label">Partitions</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-value">${topicDetails.replicationFactor}</div>
                            <div class="stat-label">Replication Factor</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-value">${topicDetails.consumerGroups ? topicDetails.consumerGroups.length : 0}</div>
                            <div class="stat-label">Consumer Groups</div>
                        </div>
                    </div>

                    <div class="card">
                        <div class="card-header">Partitions</div>
                        <div class="card-body">
                            <div class="table-container">
                                <table>
                                    <thead>
                                        <tr>
                                            <th>Partition</th>
                                            <th>Leader</th>
                                            <th>Replicas</th>
                                            <th>ISR</th>
                                            <th>Earliest Offset</th>
                                            <th>Latest Offset</th>
                                            <th>Messages</th>
                                            <th>Actions</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        ${topicDetails.partitions.map(partition => `
                                            <tr>
                                                <td><strong>${partition.partitionId}</strong></td>
                                                <td>Broker ${partition.leaderId}</td>
                                                <td>${partition.replicas ? partition.replicas.join(', ') : 'N/A'}</td>
                                                <td>${partition.inSyncReplicas ? partition.inSyncReplicas.join(', ') : 'N/A'}</td>
                                                <td>${partition.earliestOffset}</td>
                                                <td>${partition.latestOffset}</td>
                                                <td>${partition.messageCount}</td>
                                                <td>
                                                    <button class="btn btn-primary btn-sm" onclick="kaffyUI.loadPartitionMessagesPage('${topicDetails.topicName}', ${partition.partitionId})">View Messages</button>
                                                </td>
                                            </tr>
                                        `).join('')}
                                    </tbody>
                                </table>
                            </div>
                        </div>
                    </div>

                    <div class="card">
                        <div class="card-header">Configuration</div>
                        <div class="card-body">
                            <table>
                                ${Object.entries(topicDetails.configurations || {}).map(([key, value]) => `
                                    <tr><td><strong>${key}:</strong></td><td>${value}</td></tr>
                                `).join('')}
                            </table>
                        </div>
                    </div>
                </div>
            </div>
        `;
    }

    getCreateTopicModalHTML() {
        return `
            <div id="create-topic-modal" class="modal">
                <div class="modal-content">
                    <div class="modal-header">
                        <h3>Create Topic</h3>
                        <span class="close" onclick="kaffyUI.hideCreateTopicModal()">&times;</span>
                    </div>
                    <div class="modal-body">
                        <form id="create-topic-form">
                            <div class="form-group">
                                <label class="form-label">Topic Name</label>
                                <input type="text" id="new-topic-name" class="form-control" required>
                            </div>
                            <div class="form-group">
                                <label class="form-label">Partitions</label>
                                <input type="number" id="new-topic-partitions" class="form-control" value="1" min="1" required>
                            </div>
                            <div class="form-group">
                                <label class="form-label">Replication Factor</label>
                                <input type="number" id="new-topic-replication" class="form-control" value="1" min="1" required>
                            </div>
                        </form>
                    </div>
                    <div class="modal-footer">
                        <button class="btn btn-success" onclick="kaffyUI.createTopic()">Create</button>
                        <button class="btn" onclick="kaffyUI.hideCreateTopicModal()">Cancel</button>
                    </div>
                </div>
            </div>
        `;
    }

    getLoadingHTML() {
        return `
            <div class="loading">
                <div class="spinner"></div>
                <p>Loading...</p>
            </div>
        `;
    }



    getErrorHTML(message) {
        return `
            <div class="card">
                <div class="card-body">
                    <div style="text-align: center; color: #e74c3c; padding: 2rem;">
                        <h3>Error</h3>
                        <p>${message}</p>
                    </div>
                </div>
            </div>
        `;
    }

    getHealthClass(topic) {
        if (topic.underReplicatedPartitions > 0) return 'health-error';
        if (topic.preferredPercentage < 100) return 'health-warning';
        return 'health-good';
    }

    setupTopicSearch() {
        const searchInput = document.getElementById('topic-search');
        const table = document.getElementById('topics-table');
        
        searchInput.addEventListener('input', (e) => {
            const searchTerm = e.target.value.toLowerCase();
            const rows = table.querySelectorAll('tbody tr');
            
            rows.forEach(row => {
                const topicName = row.getAttribute('data-topic').toLowerCase();
                row.style.display = topicName.includes(searchTerm) ? '' : 'none';
            });
        });
    }

    setupProducerForm() {
        const form = document.getElementById('producer-form');
        form.addEventListener('submit', async (e) => {
            e.preventDefault();
            
            const topic = document.getElementById('producer-topic').value;
            const key = document.getElementById('producer-key').value;
            const value = document.getElementById('producer-value').value;
            
            try {
                const response = await fetch(this.baseUrl + '/send/', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    body: JSON.stringify({
                        topicName: topic,
                        key: key || null,
                        value: value
                    })
                });
                
                if (response.ok) {
                    this.showNotification('Message sent successfully!', 'success');
                    form.reset();
                } else {
                    this.showNotification('Failed to send message', 'error');
                }
            } catch (error) {
                this.showNotification('Error sending message: ' + error.message, 'error');
            }
        });
    }

    setupPartitionViewer(topicName) {
        // Add click handlers for partition cards if needed
    }

    setupDashboardSearch() {
        const searchInput = document.getElementById('dashboard-topic-search');
        
        if (searchInput) {
            searchInput.addEventListener('input', (e) => {
                const searchTerm = e.target.value.toLowerCase();
                
                // Find all tables and look for the one with topic data
                const tables = document.querySelectorAll('table');
                tables.forEach(table => {
                    const headerRow = table.querySelector('thead tr');
                    if (headerRow && headerRow.textContent.includes('Topic Name')) {
                        const rows = table.querySelectorAll('tbody tr');
                        rows.forEach(row => {
                            const firstCell = row.querySelector('td:first-child');
                            if (firstCell) {
                                const topicName = firstCell.textContent.toLowerCase();
                                row.style.display = topicName.includes(searchTerm) ? '' : 'none';
                            }
                        });
                    }
                });
            });
        }
    }



    showProducer() {
        this.loadProducer();
    }

    async loadPartitionMessages(topicName, partition) {
        try {
            const messages = await this.fetchData(`/topics/${topicName}/partitions/${partition}/messages`);
            this.showMessagesModal(topicName, partition, messages);
        } catch (error) {
            this.showNotification('Failed to load messages: ' + error.message, 'error');
        }
    }

    async loadPartitionMessagesPage(topicName, partition) {
        // Change URL without page reload
        window.history.pushState({view: 'messages', topicName, partition}, '', `/messages/${topicName}/${partition}`);
        
        const content = document.getElementById('main-content');
        content.innerHTML = this.getLoadingHTML();

        try {
            const messages = await this.fetchData(`/topics/${topicName}/partitions/${partition}/messages`);
            content.innerHTML = this.getPartitionMessagesPageHTML(topicName, partition, messages);
        } catch (error) {
            content.innerHTML = this.getErrorHTML(`Failed to load messages for ${topicName} partition ${partition}`);
        }
    }

    getPartitionMessagesPageHTML(topicName, partition, messages) {
        return `
            <div class="card">
                <div class="card-header">
                    Messages - ${topicName} (Partition ${partition})
                    <button class="btn btn-primary" style="float: right;" onclick="kaffyUI.loadTopicDetails('${topicName}')">Back to Topic</button>
                </div>
                <div class="card-body">
                    <div class="stats-grid" style="margin-bottom: 1.5rem;">
                        <div class="stat-card">
                            <div class="stat-value">${messages.length}</div>
                            <div class="stat-label">Total Messages</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-value">${partition}</div>
                            <div class="stat-label">Partition</div>
                        </div>
                    </div>
                    
                    <div class="message-list">
                        ${messages.length > 0 ? messages.map(msg => `
                            <div class="message-item" onclick="kaffyUI.loadMessageDetails('${topicName}', ${partition}, ${msg.offset})">
                                <div class="message-header">
                                    <span class="message-offset">Offset: ${msg.offset}</span>
                                    <span class="message-timestamp">${new Date(msg.timestamp).toLocaleString()}</span>
                                </div>
                                <div class="message-key">
                                    <strong>Key:</strong> ${msg.key || 'null'}
                                </div>
                                <div class="message-preview">
                                    <strong>Value:</strong> ${this.truncateText(msg.value, 200)}
                                </div>
                                <div class="message-click-hint">
                                    <small>Click to view full message details</small>
                                </div>
                            </div>
                        `).join('') : '<div class="empty-state"><h3>No Messages Found</h3><p>This partition contains no messages.</p></div>'}
                    </div>
                </div>
            </div>
        `;
    }

    showMessagesModal(topicName, partition, messages) {
        const modal = document.createElement('div');
        modal.className = 'modal';
        modal.style.display = 'block';
        modal.innerHTML = `
            <div class="modal-content">
                <div class="modal-header">
                    <h3>Messages - ${topicName} (Partition ${partition})</h3>
                    <span class="close" onclick="this.closest('.modal').remove()">&times;</span>
                </div>
                <div class="modal-body">
                    <div class="message-viewer">
                        ${messages.map(msg => `
                            <div class="message-item" style="margin-bottom: 1rem; padding: 0.5rem; border: 1px solid #ddd; cursor: pointer;" onclick="kaffyUI.loadMessageDetails('${topicName}', ${partition}, ${msg.offset})">
                                <strong>Offset:</strong> ${msg.offset}<br>
                                <strong>Key:</strong> ${msg.key || 'null'}<br>
                                <strong>Timestamp:</strong> ${new Date(msg.timestamp).toLocaleString()}<br>
                                <strong>Value Preview:</strong> ${this.truncateText(msg.value, 100)}<br>
                                <small style="color: #666;">Click to view full message details</small>
                            </div>
                        `).join('')}
                    </div>
                </div>
            </div>
        `;
        document.body.appendChild(modal);
    }

    async loadMessageDetails(topicName, partition, offset) {
        // Close any open modals
        const modals = document.querySelectorAll('.modal');
        modals.forEach(modal => modal.remove());
        
        // Change URL without page reload
        window.history.pushState({view: 'message', topicName, partition, offset}, '', `/message/${topicName}/${partition}/${offset}`);
        
        const content = document.getElementById('main-content');
        content.innerHTML = this.getLoadingHTML();

        try {
            const message = await this.fetchData(`/message/${topicName}/${partition}/${offset}`);
            content.innerHTML = this.getMessageDetailsHTML(message);
        } catch (error) {
            content.innerHTML = this.getErrorHTML(`Failed to load message details`);
        }
    }

    getMessageDetailsHTML(message) {
        return `
            <div class="card">
                <div class="card-header">
                    Message Details - ${message.topic} (Partition ${message.partition}, Offset ${message.offset})
                    <button class="btn btn-primary" style="float: right;" onclick="kaffyUI.loadTopicDetails('${message.topic}')">Back to Topic</button>
                </div>
                <div class="card-body">
                    <div class="message-details">
                        <div class="detail-section">
                            <h4>Message Information</h4>
                            <table class="detail-table">
                                <tr><td><strong>Topic:</strong></td><td>${message.topic}</td></tr>
                                <tr><td><strong>Partition:</strong></td><td>${message.partition}</td></tr>
                                <tr><td><strong>Offset:</strong></td><td>${message.offset}</td></tr>
                                <tr><td><strong>Key:</strong></td><td>${message.key || 'null'}</td></tr>
                                <tr><td><strong>Timestamp:</strong></td><td>${new Date(message.timestamp).toLocaleString()}</td></tr>
                            </table>
                        </div>

                        ${message.headers && Object.keys(message.headers).length > 0 ? `
                            <div class="detail-section">
                                <h4>Headers</h4>
                                <div class="json-viewer">
                                    <button class="expand-btn" onclick="kaffyUI.toggleJsonExpand(this)">Expand JSON</button>
                                    <pre class="json-content collapsed">${JSON.stringify(message.headers, null, 2)}</pre>
                                </div>
                            </div>
                        ` : ''}

                        <div class="detail-section">
                            <h4>Message Value</h4>
                            <div class="json-viewer">
                                <button class="expand-btn" onclick="kaffyUI.toggleJsonExpand(this)">Expand JSON</button>
                                <pre class="json-content collapsed">${this.formatJsonValue(message.value)}</pre>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        `;
    }

    formatJsonValue(value) {
        try {
            const parsed = JSON.parse(value);
            return JSON.stringify(parsed, null, 2);
        } catch (e) {
            return value;
        }
    }

    toggleJsonExpand(button) {
        const jsonContent = button.nextElementSibling;
        const isCollapsed = jsonContent.classList.contains('collapsed');
        
        if (isCollapsed) {
            jsonContent.classList.remove('collapsed');
            button.textContent = 'Collapse JSON';
        } else {
            jsonContent.classList.add('collapsed');
            button.textContent = 'Expand JSON';
        }
    }

    truncateText(text, maxLength) {
        if (!text) return 'null';
        if (text.length <= maxLength) return text;
        return text.substring(0, maxLength) + '...';
    }

    loadCreateTopic() {
        window.history.pushState({view: 'createTopic'}, '', '/create-topic');
        
        const content = document.getElementById('main-content');
        content.innerHTML = this.getCreateTopicPageHTML();
        this.setupCreateTopicForm();
    }

    getCreateTopicPageHTML() {
        return `
            <div class="card">
                <div class="card-header">
                    Create New Topic
                    <button class="btn btn-primary" style="float: right;" onclick="kaffyUI.loadDashboard()">Back to Dashboard</button>
                </div>
                <div class="card-body">
                    <form id="create-topic-form">
                        <div class="form-group">
                            <label class="form-label">Topic Name</label>
                            <input type="text" id="new-topic-name" class="form-control" required>
                        </div>
                        <div class="form-group">
                            <label class="form-label">Partitions</label>
                            <input type="number" id="new-topic-partitions" class="form-control" value="1" min="1" required>
                        </div>
                        <div class="form-group">
                            <label class="form-label">Replication Factor</label>
                            <input type="number" id="new-topic-replication" class="form-control" value="1" min="1" required>
                        </div>
                        <button type="submit" class="btn btn-success">Create Topic</button>
                        <button type="button" class="btn" onclick="kaffyUI.loadDashboard()" style="margin-left: 1rem;">Cancel</button>
                    </form>
                </div>
            </div>
        `;
    }

    setupCreateTopicForm() {
        const form = document.getElementById('create-topic-form');
        form.addEventListener('submit', async (e) => {
            e.preventDefault();
            await this.createTopic();
        });
    }

    async createTopic() {
        const name = document.getElementById('new-topic-name').value;
        const partitions = parseInt(document.getElementById('new-topic-partitions').value);
        const replication = parseInt(document.getElementById('new-topic-replication').value);

        try {
            const response = await fetch(this.baseUrl + '/createTopic', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    topicName: name,
                    noOfPartitions: partitions,
                    replicas: replication
                })
            });

            if (response.ok) {
                this.showNotification('Topic created successfully!', 'success');
                this.loadDashboard();
            } else {
                this.showNotification('Failed to create topic', 'error');
            }
        } catch (error) {
            this.showNotification('Error creating topic: ' + error.message, 'error');
        }
    }

    async deleteTopic(topicName) {
        if (confirm(`Are you sure you want to delete topic "${topicName}"?`)) {
            try {
                const response = await fetch(this.baseUrl + '/deleteTopic?topicName=' + encodeURIComponent(topicName), {
                    method: 'POST'
                });

                if (response.ok) {
                    this.showNotification('Topic deleted successfully!', 'success');
                    this.loadTopics();
                } else {
                    this.showNotification('Failed to delete topic', 'error');
                }
            } catch (error) {
                this.showNotification('Error deleting topic: ' + error.message, 'error');
            }
        }
    }
}

// Initialize the UI when the page loads
let kaffyUI;
document.addEventListener('DOMContentLoaded', () => {
    kaffyUI = new KaffyUI();
    
    // Handle browser back/forward buttons
    window.addEventListener('popstate', (event) => {
        if (event.state) {
            if (event.state.view === 'dashboard') {
                kaffyUI.loadDashboard();
            } else if (event.state.view === 'topic') {
                kaffyUI.loadTopicDetails(event.state.topicName);
            } else if (event.state.view === 'createTopic') {
                kaffyUI.loadCreateTopic();
            } else if (event.state.view === 'message') {
                kaffyUI.loadMessageDetails(event.state.topicName, event.state.partition, event.state.offset);
            } else if (event.state.view === 'messages') {
                kaffyUI.loadPartitionMessagesPage(event.state.topicName, event.state.partition);
            }
        }
    });
});