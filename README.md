# HFT Base Lite

A simplified high-frequency trading system focused on core functionality without complex features like GPU acceleration, reinforcement learning, or advanced filtering. **API tokens are pre-configured and ready to use.** This streamlined version provides:

## Key Features

✅ **Core Trading Functions**: Reliable opening and closing of positions  
✅ **Multi-Symbol Support**: Simultaneous trading across multiple forex pairs  
✅ **Basic Tick & DOM Data**: Essential market data collection and processing  
✅ **Latency Monitoring**: Real-time performance tracking and metrics  
✅ **Fixed SL/TP**: Simple, consistent risk management (10 pip SL, 20 pip TP)  
✅ **Threading Support**: Efficient multi-symbol processing  
✅ **Basic Filtering**: Simple spread and confidence-based filters  
✅ **Performance Logging**: Comprehensive win rate and latency metrics  
✅ **Data Logging**: Separate tick and DOM data files with timestamp analysis  

## What's Removed from Original

❌ GPU/CUDA acceleration  
❌ Reinforcement learning (RL) components  
❌ Complex confluence analysis  
❌ Advanced scaling managers  
❌ Complex exit logic managers  
❌ Weighted tick analysis  
❌ Advanced filtering systems  

## Quick Start

### 1. Setup Environment

```bash
# Navigate to project directory
cd HFT_BASE_LITE

# Install dependencies
pip install -r requirements.txts