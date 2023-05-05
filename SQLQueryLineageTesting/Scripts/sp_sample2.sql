select
  ab.AccountNumber,
  ab.DateFrom as CollectionStartDate
into #CollectionStartDates
from AccountBalance ab
left outer join AccountBalance abprev on abprev.AccountNumber = ab.AccountNumber and abprev.DateUntil = ab.DateFrom
union
select
  ab.AccountNumber,
  ab.DateFrom
from AccountBalance ab 
  left outer join AccountBalance abprev on abprev.AccountNumber = ab.AccountNumber and abprev.DateUntil = ab.DateFrom
inner join DebtCollectionCase dcc on dcc.DebtCollectionCaseNumber = ab.DebtCollectionCaseNumber