DIR = '''
SELECT *
FROM
  (SELECT A.OPERATOR,
    A.ATA,
    ROUND(CAST(A.COUNT_FI AS float)/
    (SELECT COUNT(AU.AU_DATE)
    FROM MAIN_AIRCRAFT_MONTHLY_UTILIZATION AU
    WHERE (AU.AU_DATE BETWEEN '{0}' AND '{1}')
    AND AU.OPERATOR_CODE                =A.OPERATOR_CODE
    ), 4) AS DIR
  FROM
    (SELECT OPERATORS.OPERATOR_NAME AS OPERATOR,
      OPERATORS.OPERATOR_CODE AS OPERATOR_CODE,
      CONCAT(FI.ATA_CAUSE_CHAPTER, FI.ATA_CAUSE_SECTION) AS ATA,
      COUNT(FI.AC_MODEL)      AS COUNT_FI
    FROM MAIN_FLIGHT_INTERRUPTIONS FI LEFT JOIN MAIN_OPERATORS OPERATORS on FI.OPERATOR_CODE=OPERATORS.OPERATOR_CODE
    WHERE (FI.FI_DATE BETWEEN '{0}' AND '{1}')
    AND FI.CHARGEABLE                  ='Y'
    AND FI.AC_MODEL in {2}
    GROUP BY 
      OPERATORS.OPERATOR_NAME,
      OPERATORS.OPERATOR_CODE,
      FI.ATA_CAUSE_CHAPTER,
      FI.ATA_CAUSE_SECTION
    ) A
  ) as b PIVOT( SUM(DIR) FOR operator IN (
  [Air Nostrum],
  [Air Wisconsin],
  [China Express Airlines],
  [CityJet],
  [Endeavor Air],
  [Garuda Indonesia],
  [GoJet Airlines],
  [IBEX Airlines],
  [IrAero],
  [Jazz Aviation],
  [Lufthansa CityLine],
  [Melair],
  [Mesa Airlines],
  [PSA Airlines],
  [Rwandair Express],
  [Skywest],
  [Xfly (formerly Regional Jet OU)])) as pt;
'''

IR = ''''''

operators_regions = '''
SELECT DISTINCT OPERATOR_NAME, CONTINENT_CODE AS REGION
FROM
    (SELECT OPERATORS.OPERATOR_NAME , COUNTRY_CODE
    FROM
        (SELECT DISTINCT(FI.OPERATOR_CODE)
        FROM
            MAIN_FLIGHT_INTERRUPTIONS FI
        WHERE (FI.FI_DATE BETWEEN '2015-01-01' AND GETDATE())) FI,
        MAIN_OPERATORS OPERATORS
    WHERE FI.OPERATOR_CODE=OPERATORS.OPERATOR_CODE) A LEFT JOIN
    MAIN_COUNTRIES COUNTRIES ON A.COUNTRY_CODE=COUNTRIES.COUNTRY_CODE;'''

REMOVAL_RATE = '''
SELECT *
FROM
    (SELECT REPLACE(A.ATA, '.0', '') AS ATA, OPERATORS.OPERATOR_NAME, Case when B.FLIGHT_HOURS=0 THEN null else 1000*A.REMOVAL_NUMBER/B.FLIGHT_HOURS end as removal_rate
    FROM
        (SELECT RV.OPERATOR_CODE, count(RV.REMOVED_PART_SERIAL_NO) AS REMOVAL_NUMBER, RV.ATA_REPORTED AS ATA
        FROM
            MAIN_COMPONENT_REMOVALS RV
        WHERE RV.CR_DATE BETWEEN '{0}' AND '{1}' AND 
            RV.ATA_REPORTED IS NOT NULL AND
            RV.AC_MODEL IN {2}
        GROUP BY RV.OPERATOR_CODE, 
        RV.ATA_REPORTED
) A RIGHT JOIN
        (SELECT SUM(AU.FLIGHT_HOURS_MONTH) AS FLIGHT_HOURS, AU.OPERATOR_CODE
        FROM MAIN_AIRCRAFT_MONTHLY_UTILIZATION AU
        WHERE AU.AU_DATE BETWEEN '{0}' AND '{1}'
        GROUP BY AU.OPERATOR_CODE
) B
        ON A.OPERATOR_CODE=B.OPERATOR_CODE
        LEFT JOIN
        MAIN_OPERATORS OPERATORS
        ON OPERATORS.OPERATOR_CODE=B.OPERATOR_CODE
    ) AS C
    PIVOT( SUM(removal_rate) FOR OPERATOR_NAME IN (
    [Air Nostrum],
    [Air Wisconsin],
    [China Express Airlines],
    [CityJet],
    [Endeavor Air],
    [Garuda Indonesia],
    [GoJet Airlines],
    [IBEX Airlines],
    [IrAero],
    [Jazz Aviation],
    [Lufthansa CityLine],
    [Melair],
    [Mesa Airlines],
    [PSA Airlines],
    [Rwandair Express],
    [Skywest],
    [Xfly (formerly Regional Jet OU)])) AS PT
    order by [IBEX Airlines] desc;
'''
