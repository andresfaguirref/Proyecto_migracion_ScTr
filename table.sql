drop table [dbo].[migrados];

CREATE TABLE [dbo].[migrados](
	[tocid] [int] NOT NULL,
	[path] [varchar](4000),
 CONSTRAINT [PK_migrados] PRIMARY KEY  
(
	[tocid] ASC
)
);

ALTER TABLE [dbo].[migrados] add CONSTRAINT [FK_migrados_toc] FOREIGN KEY([tocid])
REFERENCES [dbo].[toc] ([tocid]);

insert into [dbo].[migrados] values (1, '/');




select distinct s.pset_id, s.pset_name, d.prop_id, d.prop_name, d.prop_type
from [dbo].[propset] s
 join dbo.toc t on s.pset_id = t.pset_id
 join dbo.propval v on v.tocid = t.tocid
 join dbo.propdef d on v.prop_id = d.prop_id
--order by s.pset_id, d.prop_id


select distinct s.pset_id, s.pset_name, d.prop_id, d.prop_name, d.prop_type
from [dbo].[propset] s
 join dbo.[pset_props] r on s.pset_id = r.pset_id
 join dbo.propdef d on r.prop_id = d.prop_id
order by s.pset_id, d.prop_id