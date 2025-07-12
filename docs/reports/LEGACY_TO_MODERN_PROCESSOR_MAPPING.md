# 🗺️ **LEGACY TO MODERN PROCESSOR MAPPING**

This document maps the 47+ legacy SQL-based strategies to their corresponding modern processor implementations.

## 📊 **MAPPING OVERVIEW**

| Status | Legacy SQL Strategy | Modern Processor | Implementation Status |
|--------|-------------------|------------------|---------------------|
| ✅ | `book_conflicts_strategy_postgres.sql` | `BookConflictProcessor` | **IMPLEMENTED** |
| ✅ | `public_money_fade_strategy_postgres.sql` | `PublicFadeProcessor` | **IMPLEMENTED** |
| ✅ | `late_sharp_flip_strategy_postgres.sql` | `LateFlipProcessor` | **IMPLEMENTED** |
| ✅ | `consensus_moneyline_strategy_postgres.sql` | `ConsensusProcessor` | **IMPLEMENTED** |
| ✅ | `line_movement_strategy_postgres.sql` | `LineMovementProcessor` | **IMPLEMENTED** |
| ✅ | `underdog_ml_value_strategy_postgres.sql` | `UnderdogValueProcessor` | **IMPLEMENTED** |
| ✅ | `opposing_markets_strategy_postgres.sql` | `OpposingMarketsProcessor` | **IMPLEMENTED** |
| ✅ | `sharp_action_detector_postgres.sql` | `SharpActionProcessor` | **IMPLEMENTED** |
| ✅ | `hybrid_line_sharp_strategy_postgres.sql` | `HybridSharpProcessor` | **IMPLEMENTED** |
| ✅ | `timing_based_strategy_postgres.sql` | `TimingBasedProcessor` | **IMPLEMENTED** |
| 🔄 | `total_line_sweet_spots_strategy_postgres.sql` | `TotalSweetSpotsProcessor` | **NEEDS CREATION** |
| 🔄 | `team_specific_bias_strategy_postgres.sql` | `TeamBiasProcessor` | **NEEDS CREATION** |
| 🔄 | `signal_combinations_postgres.sql` | `SignalCombinationProcessor` | **NEEDS CREATION** |
| 🔄 | `enhanced_late_sharp_flip_strategy_postgres.sql` | `EnhancedLateFlipProcessor` | **NEEDS CREATION** |
| 🔄 | `strategy_comparison_roi_postgres.sql` | `ROIComparisonProcessor` | **NEEDS CREATION** |
| 🔄 | `consensus_signals_current_postgres.sql` | `CurrentConsensusProcessor` | **NEEDS CREATION** |
| 🔄 | `executive_summary_report_postgres.sql` | `ExecutiveSummaryProcessor` | **NEEDS CREATION** |

---

## 🎯 **DETAILED STRATEGY MAPPING**

### **✅ ALREADY IMPLEMENTED (10/15)**

#### 1. **Book Conflicts Strategy** 
- **Legacy**: `book_conflicts_strategy_postgres.sql` (21KB, 371 lines)
- **Modern**: `BookConflictProcessor` 
- **Purpose**: Detects line discrepancies across different sportsbooks
- **Key Logic**: Identifies arbitrage and line shopping opportunities

#### 2. **Public Money Fade Strategy**
- **Legacy**: `public_money_fade_strategy_postgres.sql` (8.2KB, 177 lines)  
- **Modern**: `PublicFadeProcessor`
- **Purpose**: Identifies contrarian betting opportunities by fading public sentiment
- **Key Logic**: Finds games where sharp money opposes public betting patterns

#### 3. **Late Sharp Flip Strategy**
- **Legacy**: `late_sharp_flip_strategy_postgres.sql` (9.2KB, 238 lines)
- **Modern**: `LateFlipProcessor` 
- **Purpose**: Detects last-minute sharp money movements
- **Key Logic**: Identifies late-breaking professional betting activity

#### 4. **Consensus Moneyline Strategy**
- **Legacy**: `consensus_moneyline_strategy_postgres.sql` (6.5KB, 150 lines)
- **Modern**: `ConsensusProcessor`
- **Purpose**: Identifies strong consensus plays across multiple metrics
- **Key Logic**: Combines multiple signal sources for high-confidence recommendations

#### 5. **Line Movement Strategy**
- **Legacy**: `line_movement_strategy_postgres.sql` (10KB, 263 lines)
- **Modern**: `LineMovementProcessor`
- **Purpose**: Analyzes betting line movements and price action
- **Key Logic**: Tracks significant line movements indicating sharp action

#### 6. **Underdog Moneyline Value Strategy**
- **Legacy**: `underdog_ml_value_strategy_postgres.sql` (8.5KB, 201 lines)
- **Modern**: `UnderdogValueProcessor`
- **Purpose**: Identifies value betting opportunities on underdogs
- **Key Logic**: Finds mispriced underdog moneylines with positive expected value

#### 7. **Opposing Markets Strategy**
- **Legacy**: `opposing_markets_strategy_postgres.sql` (12KB, 332 lines)
- **Modern**: `OpposingMarketsProcessor`
- **Purpose**: Exploits contradictions between moneyline and spread markets
- **Key Logic**: Detects when ML and spread markets disagree on game outlook

#### 8. **Real-Time Processing**
- **Legacy**: Multiple real-time SQL queries
- **Modern**: `RealTimeProcessor`
- **Purpose**: Provides live opportunity detection
- **Key Logic**: Real-time analysis of incoming data streams

---

#### 9. **Sharp Action Detector** ✅ **IMPLEMENTED**
- **Legacy**: `sharp_action_detector_postgres.sql` (15KB, 348 lines)
- **Modern**: `SharpActionProcessor` ✅ **IMPLEMENTED**
- **Purpose**: Primary sharp action detection algorithm
- **Key Logic**: Core professional betting pattern recognition
- **Impact**: **CRITICAL** - Foundation strategy for detecting money vs bet percentage differentials

#### 10. **Hybrid Line Sharp Strategy** ✅ **IMPLEMENTED**
- **Legacy**: `hybrid_line_sharp_strategy_postgres.sql` (11KB, 271 lines)
- **Modern**: `HybridSharpProcessor` ✅ **IMPLEMENTED** 
- **Purpose**: Combines line movement with sharp action indicators
- **Key Logic**: Multi-factor sharp betting analysis with confirmation signals
- **Impact**: **HIGH** - Generates high-ROI strategies through signal correlation

#### 11. **Timing-Based Strategy** ✅ **IMPLEMENTED**
- **Legacy**: `timing_based_strategy_postgres.sql` (22KB, 421 lines)  
- **Modern**: `TimingBasedProcessor` ✅ **IMPLEMENTED**
- **Purpose**: Advanced timing analysis with 9 granular timing categories
- **Key Logic**: Sophisticated timing validation with book credibility scoring
- **Impact**: **HIGH** - Largest legacy file with most complex timing logic

---

### **🔄 NEEDS IMPLEMENTATION (4/15)**

#### 12. **Total Line Sweet Spots Strategy**
- **Legacy**: `total_line_sweet_spots_strategy_postgres.sql` (8.6KB, 243 lines)
- **Modern**: `TotalSweetSpotsProcessor` ⚠️ **MISSING**
- **Purpose**: Identifies optimal total (over/under) betting opportunities
- **Key Logic**: Finds mispriced totals at specific line values

#### 13. **Team-Specific Bias Strategy**
- **Legacy**: `team_specific_bias_strategy_postgres.sql` (11KB, 279 lines)
- **Modern**: `TeamBiasProcessor` ⚠️ **MISSING**
- **Purpose**: Exploits team-specific market biases
- **Key Logic**: Identifies teams consistently over/under-valued by public

#### 14. **Signal Combinations Strategy**
- **Legacy**: `signal_combinations_postgres.sql` (12KB, 282 lines)  
- **Modern**: `SignalCombinationProcessor` ⚠️ **MISSING**
- **Purpose**: Combines multiple betting signals for enhanced accuracy
- **Key Logic**: Multi-signal aggregation and weighting

#### 15. **Enhanced Late Sharp Flip Strategy**
- **Legacy**: `enhanced_late_sharp_flip_strategy_postgres.sql` (13KB, 275 lines)
- **Modern**: `EnhancedLateFlipProcessor` ⚠️ **MISSING**
- **Purpose**: Advanced version of late flip detection
- **Key Logic**: More sophisticated late-money detection algorithms

---

## 🎯 **IMPLEMENTATION PRIORITY**

### **🔥 PHASE 1A: CRITICAL PROCESSORS (3)**
1. **SharpActionProcessor** - Core sharp detection (15KB legacy)
2. **HybridSharpProcessor** - Multi-factor analysis (11KB legacy)  
3. **TimingBasedProcessor** - Optimal timing (22KB legacy - largest file!)

### **📊 PHASE 1B: HIGH-VALUE PROCESSORS (2)**
4. **TotalSweetSpotsProcessor** - Total line optimization
5. **TeamBiasProcessor** - Team-specific inefficiencies

### **⚡ PHASE 1C: ADVANCED PROCESSORS (2)**  
6. **SignalCombinationProcessor** - Multi-signal aggregation
7. **EnhancedLateFlipProcessor** - Advanced flip detection

---

## 📈 **EXPECTED IMPACT**

### **Current State**: 10/15 Processors (66.7% Coverage)
- **Legacy Results**: 47 strategies with high ROI
- **Modern Results**: 1 profitable strategy found

### **After Full Implementation**: 15/15 Processors (100% Coverage)
- **Expected Results**: Full parity with 47+ legacy strategies
- **Modern Advantages**: 
  - ✅ Clean object-oriented architecture
  - ✅ Unified confidence scoring
  - ✅ Enhanced error handling
  - ✅ Real-time processing capabilities
  - ✅ Better testing and validation

---

## 🚀 **NEXT STEPS**

1. **Create Missing Processors** (Phase 1A-1C)
2. **Update Strategy Factory** to include all processors
3. **Run Full Backtesting** on complete processor set
4. **Validate Results** match legacy system performance
5. **Decommission Legacy System** once parity achieved

---

*This mapping ensures zero functionality loss during the legacy system decommissioning.* 