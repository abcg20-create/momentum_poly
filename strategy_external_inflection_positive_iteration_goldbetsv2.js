(function () {
  'use strict';

  const BaseStrategy = require('./strategy_external_inflection_positive_iteration.js');
  const DEFAULT_PROFILE = 'gold_bets_v2';

  function withDefaultProfile(params) {
    return {
      ...(params && typeof params === 'object' ? params : {}),
      sizingProfile: String(
        (params && typeof params === 'object' && params.sizingProfile) || DEFAULT_PROFILE
      ).trim() || DEFAULT_PROFILE,
    };
  }

  const TradeStrategy = {
    ...BaseStrategy,
    VERSION: `${String(BaseStrategy.VERSION || 'INFLECTION_POSITIVE_ITERATION')}_GOLDBETSV2`,
    makeStrategy(params) {
      return BaseStrategy.makeStrategy(withDefaultProfile(params));
    },
  };

  if (typeof module !== 'undefined' && module.exports) {
    module.exports = TradeStrategy;
  }
  if (typeof window !== 'undefined') {
    window.TradeStrategy = TradeStrategy;
  }
})();
