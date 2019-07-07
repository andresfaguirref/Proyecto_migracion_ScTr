CREATE TABLE [dbo].[migrados](
	[tocid] [int] NOT NULL,
	[path] [varchar](1000),
 CONSTRAINT [PK_migrados] PRIMARY KEY  
(
	[tocid] ASC
)
);

ALTER TABLE [dbo].[migrados] add CONSTRAINT [FK_migrados_toc] FOREIGN KEY([tocid])
REFERENCES [dbo].[toc] ([tocid]);

insert into [dbo].[migrados] values (1, '/');