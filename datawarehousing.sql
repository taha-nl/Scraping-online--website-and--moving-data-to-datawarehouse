CREATE TABLE IF NOT EXISTS public.Property_Dim
(
    property_key bigserial NOT NULL,
    age text,
    salon text,
    etage text,
    descriptions text,
    PRIMARY KEY (property_key)
);

CREATE TABLE IF NOT EXISTS public.Location_Dim
(
    location_key bigserial NOT NULL,
    city text,
    region text,
    country text,
    secteur text,
    PRIMARY KEY (location_key)
);

CREATE TABLE IF NOT EXISTS public.Type_Dim
(
    type_key bigserial NOT NULL,
    typo text,
    PRIMARY KEY (type_key)
);

CREATE TABLE IF NOT EXISTS public.Sales_Fact
(
    property_key integer,
    location_key integer,
    type_key integer,
    price numeric,
    surface numeric
);

ALTER TABLE IF EXISTS public.Sales_Fact
ADD CONSTRAINT property_key FOREIGN KEY (property_key)
REFERENCES public.Property_Dim (property_key);

ALTER TABLE IF EXISTS public.Sales_Fact
ADD CONSTRAINT location_key FOREIGN KEY (location_key)
REFERENCES public.Location_Dim (location_key);

ALTER TABLE IF EXISTS public.Sales_Fact
ADD CONSTRAINT type_key FOREIGN KEY (type_key)
REFERENCES public.Type_Dim (type_key);


