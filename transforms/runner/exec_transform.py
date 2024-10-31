def exec_transform(transform: Transform[T]) -> None:
    for input in transform.inputs.values():
        # Input has pinned branch - don't try to fallback
        if input.branch is not None:
            self.get_dataset_from_foundry_into_duckdb(
                input.path_or_rid,
                branch=input.branch,
            )

        else:
            try:
                # Try main branch name
                self.get_dataset_from_foundry_into_duckdb(
                    input.path_or_rid,
                    branch=self.branch_name,
                )

            except BranchNotFoundError as e:
                for branch in self.fallback_branches:
                    # Try fallbacks and map back as view if found
                    created_dataset = self.get_dataset_from_foundry_into_duckdb(
                        input.path_or_rid,
                        branch=branch,
                    )
                    self.create_view(
                        src_schema=created_dataset["schema"],
                        src_table=created_dataset["tablename"],
                        target_schema=self.branch_name,
                        target_table=created_dataset["tablename"],