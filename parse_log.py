#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# bug-report: makssych@gmail.com

import re
import geoip2.database
import os
import operator


def create_dict_by_email(log_fh):
    current_dict = {}
    for line in log_fh:
        if get_email_from_line(line):
            email = get_email_from_line(line)
            ip = get_ip_from_line(line)
            if not ip:
                continue
            country = get_country_from_ip(ip)
            if country not in [
                "Algeria", "Angola", "Benin", "Botswana", 
                "Burkina Faso", "Burundi", "Cabo Verde", 
                "Cameroon", "Central African Republic", 
                "Chad", "Comoros", "Congo, Democratic Republic of the", 
                "Congo", "Cote d'Ivoire", "Djibouti", "Egypt", 
                "Equatorial Guinea", "Eritrea", "Swaziland", "Ethiopia", "Gabon", 
                "Gambia", "Ghana", "Guinea", "Guinea-Bissau", "Kenya", "Lesotho", 
                "Liberia", "Libya", "Madagascar", "Malawi", "Mali", "Mauritania", 
                "Mauritius", "Morocco", "Mozambique", "Namibia", "Niger", "Nigeria", 
                "Rwanda", "Sao Tome and Principe", "Senegal", "Seychelles", 
                "Sierra Leone", "Somalia", "South Africa", "South Sudan", 
                "Sudan", "Tanzania", "Togo", "Tunisia", "Uganda", "Zambia", "Zimbabwe"
            ]:
                continue 
            # print("email: {:30} from IP {:16}  from Country {}".format(
            #     email, ip, country)
            #     )
            # print("In this line I find email ==> {}".format(line))
            if email in current_dict:
                current_dict[email][0] += 1
            else:
                current_dict[email] = [0, ip, country]
            continue
        # print("In this line i didnt find email ==> {}".format(line))
    return current_dict

def sort_by_count(current_dict):
    sorted_d = sorted(current_dict.items(), key=operator.itemgetter(1), reverse=True)
    return sorted_d

def get_country_from_ip(ip):
    reader = geoip2.database.Reader('/home/sych/GeoLite2-City.mmdb')
    try:
        response = reader.city(ip)
    except geoip2.errors.AddressNotFoundError:
        return None
    return response.country.name

def get_ip_from_line(line):
    match = re.search(r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}', line)
    if hasattr(match, 'group'):
        return match.group(0)
    return False

def get_email_from_line(line):
    match = re.search(r'[\w\.-]+@[\w\.-]+', line)
    if hasattr(match, 'group'):
        return match.group(0)
    return False

def main(log_file_in):
    # try:
    with open(log_file_in) as f:
        # list_new = list(generate_dicts(f))
        email_dict = create_dict_by_email(f)
        # print(email_dict)
        sort_list = sort_by_count(email_dict)
        print("\n#========================= RESULT =========================#\n")
        for line in sort_list:
            print("{}".format(line))
    # except:
        # print("Error open log file fo read : {}".format(log_file_in))

if __name__ == '__main__':
    main('/home/sych/one_more_maillog')