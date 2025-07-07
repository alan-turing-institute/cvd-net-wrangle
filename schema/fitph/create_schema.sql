

CREATE TABLE participant (
  id VARCHAR(10) NOT NULL,
  diagnosis VARCHAR(100),
  dob TIMESTAMP,
  height_m NUMERIC(5,2),
  PRIMARY KEY (id)
);

CREATE TABLE weight (
  id SERIAL NOT NULL,
  record_date TIMESTAMP NOT NULL,
  participant_id VARCHAR(10) NOT NULL,
  weight_kg NUMERIC(5,2) NOT NULL,
  PRIMARY KEY (id),
  FOREIGN KEY (participant_id) REFERENCES participant(id)
);

CREATE TABLE comorbidity (
  id SERIAL NOT NULL,
  participant_id VARCHAR(10) NOT NULL,
  comorbidity VARCHAR(100) NOT NULL,
  PRIMARY KEY (id),
  FOREIGN KEY (participant_id) REFERENCES participant(id)
);

CREATE TABLE visit (
  id SERIAL NOT NULL,
  participant_id VARCHAR(10) NOT NULL,
  week INTEGER NOT NULL,
  days_after_anchor INTEGER,
  PRIMARY KEY (id),
  FOREIGN KEY (participant_id) REFERENCES participant(id)
);

CREATE TABLE timeline_label (
  id SERIAL NOT NULL,
  label VARCHAR(10) NOT NULL,
  PRIMARY KEY (id)
);

CREATE TABLE gad7 (
  id SERIAL NOT NULL,
  score INTEGER,
  visit_id INTEGER NOT NULL,
  PRIMARY KEY (id),
  FOREIGN KEY (visit_id) REFERENCES visit(id)
);

CREATE TABLE ntp_pro_bnp (
  id SERIAL NOT NULL,
  visit_id INTEGER NOT NULL,
  NTProBNP_pg/ML NUMERIC(5,2) NOT NULL,
  PRIMARY KEY (id),
  FOREIGN KEY (visit_id) REFERENCES visit(id)
);