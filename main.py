from Functions.Functions import *
import Config.config as config





# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    simu = Simulation(config.data_path)
    simu.run_life_time()
    # for _ in range(10):
    #     simu.run_for_this_month()
    print(simu.output_full_report())
    simu.output_to_excel()
    simu.output_numb_cohort_to_excel(0, config.test_path_1)
    # print(simu.group_cohort[0].output_financial_report())