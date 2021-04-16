import luigi
from src.extract_transform import ExtractCompleted

def main():

    luigi.run(main_task_cls=ExtractCompleted, local_scheduler=False)


if __name__ == "__main__":
    main()