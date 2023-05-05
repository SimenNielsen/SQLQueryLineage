SELECT
    aa.AccountNumber,
	CASE WHEN ap.CreditLimitGranted > ISNULL(ap.CreditLimitMaximum, 0) THEN ap.CreditLimitGranted
	WHEN ap.CreditLimitMaximum > ISNULL(ap.CreditLimitGranted, 0) THEN ap.CreditLimitMaximum  END AS GrantedCreditLimit,
    ROW_NUMBER() OVER (PARTITION BY aa.AccountNumber ORDER by ap.ApplicationID DESC) AS Pos
INTO #MaxCalculatedCreditLimit
FROM BNDWH..Application (NOLOCK) ap
INNER JOIN BNDWH..AccountApplication (NOLOCK) aa ON aa.ApplicationID = ap.ApplicationID