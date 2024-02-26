from contextlib import nullcontext


def context(repo, use_transactions: bool = True, dry_run: bool = False):
    if use_transactions and not dry_run:
        return repo.transaction()
    else:
        # for a dry-run, or if no transactions are requested, use a null context
        return nullcontext()
