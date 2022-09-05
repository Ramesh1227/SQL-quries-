DECLARE @return_value INT
DECLARE @data INT
DECLARE @LogisticCountryId UNIQUEIDENTIFIER
DECLARE @CountryOfDeliveryId UNIQUEIDENTIFIER
DECLARE @ModeOfTransport_Id UNIQUEIDENTIFIER
DECLARE @IncotermId UNIQUEIDENTIFIER
DECLARE @PackingId UNIQUEIDENTIFIER
DECLARE @GarmentGroupId UNIQUEIDENTIFIER = NULL
DECLARE @CurrencyId UNIQUEIDENTIFIER = NULL
DECLARE @VersionCreatedBy NVARCHAR(50)
DECLARE @VersionCreatedTimeStamp DATETIME
DECLARE @ValidFrom DATETIME
DECLARE @Costperpcs DECIMAL(9,4)
DECLARE @Costperkg DECIMAL(9,2)
DECLARE @CO2kgpcs DECIMAL(9,5)
DECLARE @CO2kgkg DECIMAL(9,2)
DECLARE @data_count INT
DECLARE @check BIT
DECLARE @cost INT
DECLARE @Insertedcountry_count INT

SET @data_count = 0
SET @Insertedcountry_count = 0

DECLARE db_cursor CURSOR FOR
  SELECT DISTINCT [logistic country id] AS [data]
  FROM   [logisticimporttable]
  WHERE  [logistic country id] = 1186

OPEN db_cursor

FETCH next FROM db_cursor INTO @data

--Begin Transaction      
BEGIN TRAN

BEGIN try
    WHILE @@FETCH_STATUS = 0
      BEGIN
          SET @check = 1
          SET @LogisticCountryId = (SELECT lg.logisticcountryid
                                    FROM   tgeographicalcountry geo,
                                           tlogisticcountry lg
                                    WHERE
          geo.geographicalcountryid = lg.geographicalcountryid
          AND geo.geographicalcountrynumber = @data)
          --print  @LogisticCountryId 
          SET @VersionCreatedTimeStamp = (SELECT Sysdatetimeoffset())

          PRINT 'started contry level'

          IF @check = 1
            BEGIN
                PRINT 'start updating Logistic country version'

                EXEC  @return_value = [dbo].[spImportTranportCostUpdateLCVersionNumber]
                    @LogisticCountryId = @LogisticCountryId,
                    @VersionCreatedBy = 'HM/JG',
                    @VersionCreatedTimeStamp = @VersionCreatedTimeStamp
                SELECT 'Return Value' = @return_value

                PRINT 'Logistic country version create'

                SET @check = 0
                SET @Insertedcountry_count += 1
            END

          --print @VersionCreatedTimeStamp
          PRINT 'starting for'

          PRINT @data

          -----------------------------------------------------------------------
          DECLARE db_cost CURSOR local FOR
            SELECT id AS [cost]
            FROM   [logisticimporttable]
            WHERE  [logistic country id] = @data  --and [Garment Group ID] not in (24,25,26,27,28) 

          -- Open the Cursor
          OPEN db_cost

          -- 3 - Fetch the next record from the cursor
          FETCH next FROM db_cost INTO @cost

          -- Set the status for the cursor
          WHILE @@FETCH_STATUS = 0
            BEGIN
                SET @CountryOfDeliveryId = (SELECT cod.countryofdeliveryid
                                            FROM   tgeographicalcountry geo,
                                                   tcountryofdelivery cod
                                            WHERE
                geo.geographicalcountryid = cod.geographicalcountryid
                AND geo.geographicalcountrynumber = @data)
                --print 'countryID'
                --print  @CountryOfDeliveryId 
                SET @GarmentGroupId = (SELECT garmentgroupid
                                       FROM   tgarmentgroup
                                       WHERE  garmentgroupexternalid =
                                              (SELECT
                                              [garment group id]
                                                                        FROM
                                              [logisticimporttable]
                                                                        WHERE
                                              id =
                                              @cost
                                              )
                                      )
                --print  @GarmentGroupId 
                SET @ModeOfTransport_Id = (SELECT modeoftransportid
                                          FROM   tmodeoftransport
                                          WHERE
                modeoftransportcode = (SELECT [mode of transport code]
                                       FROM   [logisticimporttable]
                                       WHERE  id = @cost))

               print  @ModeOfTransport_Id 
				print @cost
                SET @IncotermId = (SELECT DISTINCT INC.incotermid
                                   FROM   tincoterm INC,
                                          tincotermversion IV
                                   WHERE  INC.incotermid = IV.incotermid
                                          AND IV.incotermname =
                                              (SELECT
                                              [incoterm]
                                                                 FROM
                                              [logisticimporttable]
                                                                 WHERE  id =
                                              @cost
                                              ))
                --print  @IncotermId
                SET @PackingId = (SELECT packingid
                                  FROM   tpacking
                                  WHERE
                packingcode = (SELECT Iif(packing = 'Flat', 1
                                      , 2)
                                      AS
                                      Packingcode
                               FROM   [logisticimporttable]
                               WHERE  id = @cost))
                --print @PackingId
                SET @CurrencyId = (SELECT currencyid
                                   FROM   tcurrency
                                   WHERE  currencyisocode = (SELECT currency
                                                             FROM
                                          [logisticimporttable]
                                                             WHERE  id = @cost))
                --print @CurrencyId
                SET @ValidFrom = (SELECT [valid from date]
                                  FROM   [logisticimporttable]
                                  WHERE  id = @cost)
                --print @ValidFrom
                SET @Costperpcs = (SELECT [cost (per pcs)]
                                   FROM   [logisticimporttable]
                                   WHERE  id = @cost)
                print @Costperpcs
                SET @Costperkg = (SELECT [cost (per kg)]
                                  FROM   [logisticimporttable]
                                  WHERE  id = @cost)

                PRINT @Costperkg

                SET @CO2kgpcs = (SELECT [co2 (kg/pcs)]
                                 FROM   [logisticimporttable]
                                 WHERE  id = @cost)
                --print @CO2kgpcs
                SET @CO2kgkg = (SELECT [co2 (kg/kg)]
                                FROM   [logisticimporttable]
                                WHERE  id = @cost)

                --print @CO2kgkg
                PRINT 'start Importing Cost'

                EXEC  [dbo].[spImportTransportCostLogisticCountry]
                    @LogisticCountryId = @LogisticCountryId,
                    @CountryOfDeliveryId =  @CountryOfDeliveryId,
                    @ModeOfTransportId = @ModeOfTransport_Id,
                    @IncotermId = @IncotermId,
                    @PackingId = @PackingId,
                    @ValidFrom = @ValidFrom,
                    @CostInPercent = NULL,
                    @Margin = NULL,
                    @CostOfPacking = NULL,
                    @GarmentGroupId = @GarmentGroupId,
                    @CurrencyId =  @CurrencyId,
                    @CostPerPcs = @Costperpcs,
                    @CostPerKg = @Costperkg,
                    @CO2PerKgPerpcs = @CO2kgpcs,
                    @CO2PerKgPerKg = @CO2kgkg,
                    @OperationTypeName = 'create'
                PRINT 'cost Imported'

                SET @data_count = @data_count + 1

                FETCH next FROM db_cost INTO @cost
            END

          SELECT 'Total Count' = @data_count

          SET @data_count = 0

          CLOSE db_cost

          DEALLOCATE db_cost

          ---------------------------------------------------------------------------------
          PRINT 'Ended contry level'

          FETCH next FROM db_cursor INTO @data
      END

    SELECT 'Total country count' = @Insertedcountry_count
END try

BEGIN catch
    ---Roll back Transaction 
    IF @@error <> 0
      BEGIN
          ROLLBACK TRAN

          DECLARE @Message  VARCHAR(max) = Error_message(),
                  @Severity INT = Error_severity(),
                  @State    SMALLINT = Error_state()

          RAISERROR(@Message,@Severity,@State)
      Print 'RollBack Has Been Performed' 
      END
END catch

IF @@trancount <> 0
  BEGIN
      --Commit Transaction
      COMMIT TRAN
  END

CLOSE db_cursor

DEALLOCATE db_cursor 



--select * from tGeographicalCountry where GeographicalCountryNumber = 1137

--select * from tLogisticCountry where GeographicalCountryID = 'CA0590B2-2696-48B5-92E3-E050C9827B26'

--select * from tLogisticCountryVersion where LogisticCountryID = '3E4AE416-18F6-4F2D-9665-0A49F14BBB14' order by VersionCreatedTimeStamp desc

--select * from tLogisticCountryVersionTransportCost where LogisticCountryVersionID = 'FF7BEC8D-C7A9-4643-ABB5-FD74E301F54D'

--SELECT distinct [Country of Delivery ID]
--      ,[Logistic Country ID]
--      ,[Garment Group ID]
--      ,[Mode of Transport Code]
--      ,[Incoterm]
--      ,[Packing]
--      ,[Valid From Date]
--  FROM [SDSTrunk].[dbo].[logisticimporttable] WHERE  [logistic country id] = 1137 