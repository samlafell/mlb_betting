# 🚀 MLB Betting System - Quick Start Guide

*This guide addresses the complex setup issues identified in [GitHub issue #35](https://github.com/samlafell/mlb_betting_program/issues/35) by providing a simple, business-user-friendly onboarding experience.*

## ⚡ One-Command Setup (Recommended)

**For business users and anyone who wants to get started quickly:**

```bash
# 1. Download or clone this project
# 2. Open terminal in the project folder
# 3. Run the setup script:
./quick-start.sh
```

**That's it!** The script will automatically:
- ✅ Install all requirements
- ✅ Start database containers  
- ✅ Set up the system
- ✅ Collect initial data
- ✅ Generate first predictions

**Time required:** 5-10 minutes | **Technical expertise:** None required

---

## 🎯 Success Indicators

You'll know the setup worked when you see:

```
🎉 Quick Start Setup Complete!
==================================================

✅ Success Indicators:
  • Database running on localhost:5433
  • Redis running on localhost:6379
  • Python dependencies installed
  • Database schema created

🎯 What's Next?
```

---

## 📊 Getting Your First Predictions

Once setup is complete, getting predictions is simple:

```bash
# Get today's betting predictions
uv run -m src.interfaces.cli quickstart predictions
```

**Expected output:**
```
🎯 Quick Predictions
Getting today's betting predictions...

📊 Prediction Summary:
Game: Yankees @ Red Sox (7:05 PM ET)
├── Strategy: Sharp Action Detector  
├── Signal: Fade Public (Yankees -1.5)
├── Confidence: 85%
├── Historical ROI: +12.3%
└── Recommended Action: Bet Red Sox +1.5

✅ Predictions generated successfully!
```

---

## 🖥️ Web Dashboard (Visual Interface)

For business users who prefer a web interface:

```bash
# Start the monitoring dashboard
uv run -m src.interfaces.cli monitoring dashboard

# Then visit: http://localhost:8080
```

The dashboard provides:
- 📊 Real-time system status
- 🎯 Current predictions
- 📈 Performance metrics
- 🔧 Manual controls (no command line needed!)

---

## 🆘 Troubleshooting

### Problem: "Command not found" or "Permission denied"

**Solution:**
```bash
# Make the script executable
chmod +x quick-start.sh

# Run it
./quick-start.sh
```

### Problem: "Docker not found"

**Solution:**
1. Install [Docker Desktop](https://docs.docker.com/get-docker/)
2. Make sure Docker is running
3. Run `./quick-start.sh` again

### Problem: "No predictions available"

This is normal and could mean:
- No MLB games scheduled today
- More data collection needed

**Solutions:**
```bash
# Collect more data
uv run -m src.interfaces.cli data collect --source action_network --real

# Try lower confidence threshold
uv run -m src.interfaces.cli quickstart predictions --confidence-threshold 0.3

# Check system status
uv run -m src.interfaces.cli quickstart validate
```

### Problem: Setup script failed

**Solutions:**
```bash
# Try step-by-step repair
./quick-start.sh --skip-docker    # Skip container setup
./quick-start.sh --skip-deps      # Skip dependency installation
./quick-start.sh --skip-data      # Skip data collection

# Or use the interactive wizard
uv run -m src.interfaces.cli quickstart setup

# Get detailed help
./quick-start.sh --help
uv run -m src.interfaces.cli quickstart validate --fix-issues
```

---

## 📱 Essential Commands for Business Users

Once your system is running, these are the only commands you need to know:

### Daily Operations
```bash
# Get today's predictions (most important!)
uv run -m src.interfaces.cli quickstart predictions

# Start web dashboard  
uv run -m src.interfaces.cli monitoring dashboard
# Then visit: http://localhost:8080
```

### Weekly Maintenance
```bash
# Collect fresh data
uv run -m src.interfaces.cli data collect --source action_network --real

# Check system health
uv run -m src.interfaces.cli quickstart validate
```

### When Something Goes Wrong
```bash
# Fix common issues automatically
uv run -m src.interfaces.cli quickstart validate --fix-issues

# Restart everything cleanly
./quick-start.sh --skip-deps
```

---

## 🎓 Understanding Your Predictions

### Confidence Levels
- **High (80-100%)**: Strong signals, recommended for larger bets
- **Medium (60-79%)**: Good signals, suitable for standard bets
- **Low (40-59%)**: Weak signals, small bets or avoid

### Strategy Types
- **Sharp Action Detector**: Follows professional betting patterns
- **Consensus Processor**: Analyzes public vs. professional money
- **Line Movement Processor**: Identifies significant line changes

### Key Metrics
- **ROI**: Historical return on investment for this strategy
- **Accuracy**: How often predictions are correct
- **Edge**: Your statistical advantage over the sportsbook

---

## 🔒 Security & Production Notes

**This quick setup is for development/testing only.**

For production use:
1. Change all default passwords in `.env`
2. Use SSL/TLS certificates
3. Configure proper security headers
4. See `docs/PRODUCTION_SECURITY_GUIDE.md`

---

## 📚 What's Next?

### For Business Users
- **Focus on**: Daily predictions and web dashboard
- **Learn more**: Check the [User Guide](USER_GUIDE.md) for advanced features
- **Get support**: Use the system validation tools when issues arise

### For Technical Users  
- **Explore**: Full [README.md](README.md) with all features
- **Customize**: Modify strategies and thresholds
- **Scale**: Production deployment guides in `docs/`

---

## 💡 Pro Tips

1. **Bookmark this command**: `uv run -m src.interfaces.cli quickstart predictions`
2. **Set up an alias**: `alias mlb-predictions="uv run -m src.interfaces.cli quickstart predictions"`
3. **Use the web dashboard**: Much easier than command line for daily use
4. **Check predictions daily**: The system works best with fresh data
5. **Start small**: Test predictions with small amounts before scaling up

---

## 🆘 Still Having Issues?

1. **Try the repair mode**: `./quick-start.sh --skip-docker --skip-deps`
2. **Use system validation**: `uv run -m src.interfaces.cli quickstart validate --fix-issues`
3. **Check the logs**: Look in `logs/` directory for detailed error messages
4. **Ask for help**: Create an issue on GitHub with your error details

---

**🎯 Remember**: This system is designed to make betting analysis accessible to everyone. You don't need to be a programmer to use it effectively!