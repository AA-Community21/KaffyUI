# Kaffy UI - Kafka Management Interface

A modern, responsive web UI for managing Kafka clusters, inspired by Kafdrop.

## Features

### 🏠 Dashboard
- **Cluster Overview**: Real-time cluster statistics and health metrics
- **Quick Stats**: Total topics, partitions, brokers, and preferred leader percentage
- **Cluster Information**: Detailed cluster metadata including controller info
- **Recent Topics**: Quick access to recently viewed topics

### 📋 Topics Management
- **Topic Listing**: Comprehensive view of all topics with search functionality
- **Health Indicators**: Visual status indicators for topic health
- **Topic Creation**: Easy topic creation with configurable partitions and replication
- **Topic Deletion**: Safe topic deletion with confirmation
- **Topic Details**: In-depth view of topic configuration and partitions

### 🔧 Partition Management
- **Partition Overview**: Visual representation of partition distribution
- **Partition Details**: Leader, replicas, ISR, and offset information
- **Message Browsing**: View messages within specific partitions
- **Offset Navigation**: Navigate through message offsets

### 🖥️ Broker Information
- **Broker Listing**: Complete broker inventory with connection details
- **Controller Identification**: Clear indication of cluster controller
- **Partition Distribution**: Partition count and percentage per broker
- **Health Status**: Broker availability and performance metrics

### 📤 Message Producer
- **Simple Interface**: Easy-to-use message publishing interface
- **Key-Value Support**: Optional key specification for messages
- **Topic Selection**: Dropdown or manual topic name entry
- **JSON Formatting**: Built-in JSON validation and formatting

## UI Components

### Navigation
- **Tab-based Navigation**: Clean, intuitive navigation between sections
- **Active State Indicators**: Clear visual feedback for current section
- **Responsive Design**: Mobile-friendly navigation with collapsible menu

### Data Visualization
- **Statistics Cards**: Key metrics displayed in easy-to-read cards
- **Health Indicators**: Color-coded status indicators (green/yellow/red)
- **Interactive Tables**: Sortable, searchable data tables
- **Real-time Updates**: Automatic data refresh capabilities

### User Experience
- **Loading States**: Smooth loading animations and progress indicators
- **Error Handling**: Graceful error messages and recovery options
- **Notifications**: Toast notifications for user actions
- **Responsive Design**: Optimized for desktop, tablet, and mobile devices

## Technical Implementation

### Frontend Stack
- **Pure JavaScript**: No external frameworks, lightweight and fast
- **CSS Grid/Flexbox**: Modern layout techniques for responsive design
- **Fetch API**: Modern HTTP client for backend communication
- **ES6+ Features**: Modern JavaScript features for clean code

### Backend Integration
- **REST API**: Clean integration with Spring Boot backend
- **CORS Support**: Proper cross-origin resource sharing configuration
- **Error Handling**: Comprehensive error handling and user feedback
- **Data Validation**: Client-side validation with server-side backup

### Styling
- **Kafdrop-inspired Design**: Familiar interface for Kafka users
- **Modern CSS**: CSS Grid, Flexbox, and CSS Variables
- **Dark/Light Elements**: Balanced color scheme for readability
- **Consistent Typography**: Clear, readable font choices

## File Structure

```
src/main/resources/
├── static/
│   ├── css/
│   │   └── kaffy-ui.css          # Main stylesheet
│   └── js/
│       └── kaffy-ui.js           # Main JavaScript application
├── templates/
│   └── index.html                # Main HTML template
└── application.yml               # Configuration
```

## Usage

1. **Start the Application**: Run the Spring Boot application
2. **Access the UI**: Navigate to `http://localhost:[port]` in your browser
3. **Explore Features**: Use the navigation tabs to explore different sections
4. **Manage Topics**: Create, view, and delete topics as needed
5. **Browse Messages**: Click on topics and partitions to view messages
6. **Produce Messages**: Use the Producer tab to send test messages

## Browser Compatibility

- **Chrome**: 70+
- **Firefox**: 65+
- **Safari**: 12+
- **Edge**: 79+

## Performance Features

- **Lazy Loading**: Data loaded on-demand for better performance
- **Caching**: Intelligent caching of frequently accessed data
- **Pagination**: Large datasets handled with pagination
- **Debounced Search**: Optimized search with debouncing

## Security Considerations

- **CORS Configuration**: Properly configured cross-origin requests
- **Input Validation**: Client-side validation for all user inputs
- **XSS Protection**: Proper escaping of user-generated content
- **Error Sanitization**: Safe error message display

## Customization

The UI is designed to be easily customizable:

- **Colors**: Modify CSS variables for theme changes
- **Layout**: Adjust grid and flexbox properties
- **Features**: Add/remove sections as needed
- **Branding**: Update headers, titles, and styling

## Future Enhancements

- **Consumer Group Management**: View and manage consumer groups
- **Schema Registry Integration**: Support for Avro/JSON schemas
- **Metrics Dashboard**: Advanced monitoring and alerting
- **Export Functionality**: Export data to CSV/JSON formats
- **Advanced Filtering**: Complex filtering and sorting options