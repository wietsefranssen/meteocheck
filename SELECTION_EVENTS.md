# AG-Grid Selection Events Implementation (Event-Driven with 2-Second Debounce)

## Overview
This implementation uses **AG-Grid's native selection events** with a **2-second debounce delay** and **visual progress indicator** to handle cell selection without causing browser updates during active selection.

## Key Features

### 1. Event-Driven Selection Detection
- **Native AG-Grid events**: Uses `cellClicked` and `cellValueChanged` events
- **Minimal polling**: Only 500ms interval to check for completed timeouts
- **Instant visual feedback**: Cells highlight immediately in the grid + progress bar
- **2-second debounce**: Updates only occur 2 seconds after selection stops

### 2. Visual Progress Indicator
- **Animated progress bar**: Shows immediately when selection starts
- **Clear messaging**: "⏳ Processing selection... (waiting 2 seconds)"
- **Automatic hiding**: Bar disappears when results are ready
- **User-friendly feedback**: Clear indication of system state

### 3. Smart Update Logic
```javascript
// Event-driven with timeout mechanism
window.currentSelection = cellData;

// Clear any existing timeout
if (window.selectionTimeout) {
    clearTimeout(window.selectionTimeout);
}

// Set 2-second timeout to mark selection as ready
window.selectionTimeout = setTimeout(() => {
    window.selectionTimeout = null;
    window.selectionReady = true;  // Flag for interval to pick up
}, 2000);

return dash_clientside.no_update;  // Wait for timeout
```

### 4. Minimal Browser Activity
- **Event-driven triggers**: Only on actual cell interactions
- **500ms check interval**: Minimal polling just to detect completed timeouts
- **Visual-only feedback**: Grid selection is instant, data processing is delayed
- **Clean timeout handling**: Proper cleanup of timers

## Benefits

1. **No Sudden Browser Updates**: Debouncing eliminates jarring updates during selection
2. **Optimal Performance**: 800ms delay allows for complete selection before processing
3. **Smooth User Experience**: Immediate visual feedback with delayed data processing
4. **Resource Efficient**: Minimal server requests, maximum responsiveness
5. **Intuitive Behavior**: Users can select freely without performance concerns

## Configuration

### Timeout Settings
- **Debounce Delay**: 2000ms (2 seconds)
- **Check Interval**: 500ms (to detect completed timeouts)
- **Update Strategy**: Only when selection stops changing for 2 seconds

### Visual Indicators
- **Progress Bar**: Animated blue striped bar during processing
- **Instant Feedback**: Grid cells highlight immediately
- **Auto-Hide**: Progress bar disappears when data is ready

## API Reference

### Selection Data Structure
Each selected cell returns:
```python
{
    "row": int,           # Row index
    "station": str,       # Station name
    "variable": str,      # Variable name  
    "value": float,       # Cell value
    "type": "range"       # Selection type
}
```

### Key Components
- `selection-check-interval`: Interval component for timeout detection (500ms)
- `selected-cells-store`: Store component for selection data
- `selection-progress-bar`: Visual feedback component
- `nan-percentage-aggrid`: AG-Grid component ID

## Implementation Files
- `src/callbacks.py`: Main selection callback logic
- `src/aggrid_table.py`: AG-Grid configuration
- `src/layout.py`: Layout with interval component

## Further Optimization Ideas

1. **Dynamic Interval Adjustment**: Slow down polling when no user activity is detected
2. **Selection Debouncing**: Add debouncing for rapid selection changes
3. **Keyboard Event Handling**: Add keyboard shortcuts for common selection operations
4. **Selection Persistence**: Save/restore selection state across sessions
