SELECT
    Contact.ROWID,
    Contact.ACCOUNTID,
    Contact.SYS_NAME,
    Contact.TITLE,
    Account.ROWID,
    Account.SYS_NAME,
    Account.BILLINGCOUNTRY,
    Account.BILLINGPOSTALCODE,
    Account.INDUSTRY,
    Account.TICKERSYMBOL,
    Contact.CONTACT_STATUS
FROM
    Contact
INNER JOIN
    Account
ON
    Contact.ACCOUNTID=Account.ROWID
WHERE
    Contact.TITLE IS NOT NULL
AND Account.TICKERSYMBOL IS NOT NULL
AND Account.billingcountry='USA' limit 5