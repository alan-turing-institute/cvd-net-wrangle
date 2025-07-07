open Types  

let participant = {
  name = "participant";
  columns = [
    { name = "id"; typ = Varchar 10; nullable = false };
    { name = "diagnosis"; typ = Varchar 100; nullable = true };
    { name = "dob"; typ = Timestamp; nullable = true };
    { name = "height_m"; typ = Numeric (5,2); nullable = true };
  ];
  constraints = [
    PrimaryKey ["id"];
  ];
}

let demography_tables = [participant]