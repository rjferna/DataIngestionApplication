DROP TABLE IF EXISTS `coincap-data-hub.ref_coincap_data.bitcoin_history_linear_regression`;
CREATE TABLE `coincap-data-hub.ref_coincap_data.bitcoin_history_linear_regression` (
  created_date DATETIME NOT NULL,
  rmse FLOAT64 
)