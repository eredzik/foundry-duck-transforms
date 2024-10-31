from datetime import datetime

from sqlmodel import Field, SQLModel


class Dataset(SQLModel, table=True):
    id:                 int                  = Field(default=None, primary_key=True)
    rid                :str
    

class DatasetIdentifier(SQLModel, table=True):
    id:           int     = Field(default=None, primary_key=True)
    rid_or_path:  str
    dataset:      Dataset = Field(foreign_key="dataset.id")

# class dataset_identifier:
#     id          Int     @id @default(autoincrement())
#     rid_or_path String
#     dataset     dataset @relation(fields: [datasetid], references: [id])
#     datasetid   Int
# }

class DatasetVersion(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)
    data_identity_id: str
    data_identity_date: datetime
# class dataset_version:
#     id Int @id @default(autoincrement())

#     data_identity_id   String
#     data_identity_date DateTime    @default(now())
#     branch             data_branch @relation(fields: [data_branchId], references: [id])
#     dataset            dataset     @relation(fields: [datasetId], references: [id])
#     datasetId          Int
#     data_branchId      Int
# }

# class data_branch:
#     id Int @id @default(autoincrement())

#     full_branch_name      String
#     sanitized_branch_name String
#     dataset_version       dataset_version[]
# }
