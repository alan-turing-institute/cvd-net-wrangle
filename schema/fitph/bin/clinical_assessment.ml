open Types

let ntpprobnp = {
  name = "ntp_pro_bnp";
  columns = [
    { name = "id"; typ = Serial; nullable = false };
    { name = "visit_id"; typ=Int; nullable = false };  
    { name = "NTProBNP_pg/ML"; typ = Numeric (5,2); nullable = false };
  ];
  constraints = [
    PrimaryKey ["id"];
    ForeignKey {
      columns = ["visit_id"];
      ref_table = "visit";
      ref_columns = ["id"];
    };
  ];
}

let clinical_assessment_tables = [ntpprobnp]
