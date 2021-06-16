/**

  Creation of the HIC schema.

*/
begin;

/**

  Process schema

*/
create schema hic_process;

-- Sintaxon catalog, codigo_ue is not unique.
create table hic_process.sintaxon (
    gid integer,
    cod_comun varchar(10),
    sintaxon varchar(100),
    codigo_ue varchar(6),
    primary key(gid)
);

\copy hic_process.sintaxon from '../../data/000_in/com_hic.csv' with csv header delimiter '|'

-- UE habitat catalog, codigo is unique
create table hic_process.ue_habitat (
    gid integer,
    codigo varchar(8),
    descripcion varchar(300),
    primary key(codigo)
);

\copy hic_process.ue_habitat from '../../data/000_in/dic_ue_a.csv' with csv header delimiter '|'

-- HIC original data
create table hic_process.hic_input (
    gid integer,
    num_total_hic integer,
    num_hic_prioritarios integer,
    num_hic_prioritarios_propuestos integer,
    num_hic_no_prioritarios integer,
    num_hic_no_prioritarios_propuestos integer,
    codigos_habitat varchar(150),
    codigos_sintaxones varchar(150),
    geom geometry(MultiPolygon, 25830),
    primary key(gid)
);

\copy hic_process.hic_input from '../../data/000_in/hic.csv' with csv header delimiter '|'

/**

  Final schema

*/
create schema hic;

-- Comunidad catalog, codigo_ue is not a key
create table hic.sintaxon_catalog (
  gid integer primary key,
  codigo_ue varchar(50),
  sintaxon varchar(150)
);

-- UE habitat catalog, codigo is key
create table hic.hic_catalog (
  codigo varchar(8) primary key,
  descripcion varchar(300)
);

-- Habitats table
create table hic.hic (
  gid integer primary key,
  codigo varchar(150) references hic.hic_catalog(codigo),
  geom geometry(MultiPolygon, 3035)
);

-- Sintaxon table
create table hic.sintaxon (
  gid integer primary key,
  codigo_ue varchar(20),
  geom geometry(MultiPolygon, 3035)
);

/**

  Process data.

*/
-- Create sintaxon catalog
insert into hic.sintaxon_catalog
with a as (
  select
    codigo_ue,
    sintaxon
  from
    hic_process.sintaxon
  order by codigo_ue, sintaxon
)
select
  row_number() over (),
  codigo_ue,
  sintaxon
from a;

-- Create HIC catalog
insert into hic.hic_catalog
select
  codigo,
  descripcion
from
  hic_process.ue_habitat
order by codigo;

-- Write sintaxon final data
insert into hic.sintaxon
with a as (
  select
    unnest(regexp_split_to_array(codigos_sintaxones, ',')) as codigo_ue,
    st_transform(geom, 3035) as geom
  from
    hic_process.hic_input
)
select
  row_number() over (),
  codigo_ue,
  geom
from a;

-- There are data inconsistencies in the HIC in some codes, create first
-- a temporary data to clean up
create table hic_process.hic_process as
with a as (
  select
    unnest(regexp_split_to_array(codigos_habitat, ', ')) as codigo,
    st_transform(geom, 3035) as geom
  from
    hic_process.hic_input
)
select
  row_number() over (),
  codigo,
  geom
from a;

-- Correct inconsistencies
update hic_process.hic_process
set codigo = '1240'
where codigo = '1240+';

delete from hic_process.hic_process
where codigo = '1340_1*';

-- Write corrected data
insert into hic.hic
select * from hic_process.hic_process;

-- Clean up the process schema
drop schema hic_process cascade;

/**

  Clean invalid geometries that crashes the gridder (~700 occurences)

*/
delete from hic.hic
where not st_isvalid(geom);

delete from hic.sintaxon
where not st_isvalid(geom);

/**

  Create production materialized views.

*/
create materialized view hic.hic_view as
select
  row_number() over () as gid,
  a.codigo,
  b.descripcion,
  a.geom
from
  hic.hic a inner join
  hic.hic_catalog b on a.codigo = b.codigo;

create materialized view hic.sintaxon_view as
select
  row_number() over () as gid,
  b.codigo_ue,
  b.sintaxon,
  a.geom
from
  hic.sintaxon a inner join
  hic.sintaxon_catalog b on split_part(a.codigo_ue, '_', 1) = b.codigo_ue;


commit;
