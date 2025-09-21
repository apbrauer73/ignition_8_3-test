UPDATE tank 
SET 
 tankName = :tankName, 
 tankType = :tankType,
 tankVol = :tankVol,
 tankMaxVol = :tankMaxVol,
 tankInUse = :tankInUse
WHERE tankNo = :tankNo