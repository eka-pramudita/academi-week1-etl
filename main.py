import luigi
from src.loader import LoaderCompleted

def main():

    luigi.run(main_task_cls=LoaderCompleted, local_scheduler=False)


if __name__ == "__main__":
    main()