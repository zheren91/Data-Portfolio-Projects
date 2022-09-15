# View table nash (Nashville Housing Dataset)
use test;
select * from nash;
desc nash;


# Enable table modification 
SET SQL_SAFE_UPDATES = 0;

# transaction for modification
set autocommit = 0;
start transaction;
# Queries to be tested before committing:



rollback;
commit;

# ----------------------------------- Updates and changes to the table
# Replace empty strings with null
update nash
set
	propertyaddress = case propertyaddress when '' then null else propertyaddress end;




# ----------------------------------- Convert Sale Date to Date format
# Change Sale Date format to YYYY-mm-dd
update nash
set saledate = str_to_date(saledate, "%M %d,%Y");

    
# ----------------------------------- Clean Property Address column 

# Check for missing property address
select * from nash where propertyaddress is null;

# ParcelID can be matched with property address
select * from nash order by parcelid;

# If parcelID's are equal but one query is missing a property address
# populate the property address using self join
# Using self join and match on ParcelID


select a.parcelid, a.propertyaddress, b.parcelid, b.propertyaddress  
from nash a join nash b
on a.parcelid = b.parcelid
and a.uniqueid <> b.uniqueid
where a.propertyaddress is null;

# Populate the null property address by filling it with 
# the propertyaddress with the same parcelID
update nash a
join nash b on a.parcelid = b.parcelid
and a.uniqueid <> b.uniqueid
set a.propertyaddress = ifnull(a.propertyaddress,b.propertyaddress) 
where a.propertyaddress is null;

# Check if there are anymore missing values in property address
select * from nash where propertyaddress is null;

# ----------------------------------- Separate property address separate columns (Address, City)

SELECT SUBSTRING_INDEX(propertyaddress, ',', 1) as Address,
SUBSTRING_INDEX(propertyaddress, ',', -1) as City
from nash;

# Add address and city columns
alter table nash
add PropertyAddressSplit varchar(255);

update nash
set PropertyAddressSplit = SUBSTRING_INDEX(propertyaddress, ',', 1);

alter table nash
add PropertyCitySplit varchar(255);

update nash
set PropertyCitySplit = SUBSTRING_INDEX(propertyaddress, ',', -1);
 
# ----------------------------------- Clean sold as vacant column

select distinct(soldasvacant), count(soldasvacant)
from nash
group by soldasvacant;

select soldasvacant,
case when soldasvacant = 'Y' then 'Yes'
	 when soldasvacant = 'N' then 'No'
     else soldasvacant end
     from nash;
     
update nash
set soldasvacant = case when soldasvacant = 'Y' then 'Yes'
	 when soldasvacant = 'N' then 'No'
     else soldasvacant end;
     
#  ----------------------------------- Determine the duplicate rows
# Create a CTE to determine the duplicates
with rowdupCTE as(
select *, 
row_number() over( 
   partition by parcelid,
				propertyaddress,
				totalvalue,
                saledate,
                legalreference
                order by
                uniqueid) rownumber
from nash
order by parcelid)
# Select the duplicates using the created CTE
select * from rowdupCTE
where rownumber > 1
order by landuse;




