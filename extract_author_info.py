#!/usr/bin/env python3

"""
Purpose of this script is to produce a plain text list of authors and associated afiiliations based on a
TARA-PACIFIC_authors-lists template.
It will take the path to the .xlsx formatted TARA-PACIFIC_authors-lists, and the name of the sheet that the extraction
should happen from.
"""

import pandas as pd
import os
from collections import defaultdict
import requests
import json
import argparse
from argparse import RawDescriptionHelpFormatter
import ntpath


class AuthorInfoExtraction:
    def __init__(self):
        self.parser = self._return_parser()
        self._define_args()
        self.args = self.parser.parse_args()
        self.excel_path = self.args.excel_path
        self.target_sheet_name = self.args.target_sheet_name
        self.output_dir = self.args.output_dir_path
        if not os.path.isdir(self.output_dir):
            raise RuntimeError(f'{self.output_dir} is not a recognized directory')
        self.submission = self.args.submission
        if self.submission: # Whether to do a submission
            self._setup_submission_vars()
        self.author_categories = ['First author(s)', 'Contributing authors list #1',
                                  'Contributing authors list #2', 'Consortium Coordinators', 'Scientific Directors',
                                  'Contributing authors list #3']
        self.author_info_df = self._make_author_info_df()
        # Generate the author order where each author is represented by the index version of their name used in the
        # self.name_df created above.
        self.author_order = self._make_author_order()

        # Now create the author to affiliation number dictionary
        # And the affiliation number to affiliation string dictionary
        (
            self.affiliation_list,
            self.affil_str_to_affil_num_dict,
            self.affil_num_to_affil_str_dict,
            self.author_to_affil_num_list_dict
        ) = self._make_affiliations_dicts()
        # The arrays of authors that will be listed for the given dataset. This will be added to the metadata.
        self.creator_array = self._make_creator_array()

    @staticmethod
    def _return_parser():
        return argparse.ArgumentParser(
            description='A script for producing an ordered author and affiliation '
                        'list from the TARA-PACIFIC_authors-lists template using the Zenodo API '
                        '(https://developers.zenodo.org/) and for creating a new Zenodo submission. '
                        'By default, the following defaults will be used for the meta data:\n'
                        '\taccess_right: restricted;\n'
                        '\tlicense: CC-BY-4.0;\n'
                        '\taccess_conditions: Any Tara Pacific Expedition participant may request access.;\n'
                        '\tcommunities: tarapacific;\n'
                        '\tversion: 1;\n'
                        '\tlanguage: eng;\n',
            epilog='For support, email: didillysquat@gmail.com', formatter_class=RawDescriptionHelpFormatter)

    def _setup_submission_vars(self):
        """
        Setup the variables taht are associated with doing the Zenodo submission.
        """
        # Get files to upload
        self.data_file_path_list = self._get_and_check_datafile_paths()

        # Title for meta info
        self.meta_title = self.args.meta_title
        if not self.meta_title:
            raise RuntimeError('Please provide a valid title')

        # Description for meta info
        self.meta_description = self.args.meta_description
        if os.path.isfile(self.meta_description):
            # This points to a file and the description should be the contents of the file
            with open(self.meta_description, 'r') as f:
                self.meta_description = '\n'.join([line.rstrip() for line in f])

        # Personal access token is required for making uploads and publishing on Zenodo using their API
        with open(self.args.access_token_path, 'r') as f:
            self.access_token = f.readline().rstrip()

        # References to be associated to the metadata
        if self.args.references:
            if not os.path.exists(self.args.references):
                raise FileNotFoundError(f'{self.args.references} not found')
            else:
                with open(self.args.references, 'r') as f:
                    self.references = [line.rstrip() for line in f]
        else:
            self.references = []

    def _get_and_check_datafile_paths(self):
        """
        Get the paths for the files that will be uploaded. Ensure that each of the files exist else
        raise FileNotFoundError.
        If no files have been provided raise RuntimeError and ask user to provide a file.
        """
        if not self.args.data_file_paths:
            print('WARNING: No data files will be uploaded with this submission')
            return []
        data_file_path_list = self.args.data_file_paths.split(',')
        if not data_file_path_list:
            print('WARNING: No data files will be uploaded with this submission')
            return []
        no_file = []
        for data_file in data_file_path_list:
            if not os.path.isfile(data_file):
                no_file.append(data_file)
        if no_file:
            file_list = '\n'.join([f'\t{file}' for file in no_file])
            raise FileNotFoundError(f'The following files for upload could not be found {file_list}')
        return data_file_path_list

    def _define_args(self):
        self.parser.add_argument(
            '--excel_path',
            help='The full path to the TARA-PACIFIC_authors_list.xlsx',
            required=True
        )
        self.parser.add_argument(
            '--target_sheet_name',
            help='The name of the excel sheet in the TARA-PACIFIC_authors_list.xlsx '
                 'that the author list should be generated for.', required=True
        )
        self.parser.add_argument(
            '--output_dir_path',
            help='Full path to the directory where output files will be written', required=True
        )

        # Required if doing a submission
        self.parser.add_argument(
            '--submission',
            action='store_true',
            help='If this is passed, a submission to Zenodo will be created.\n'
                 'If not passed, only the author and affiliation lists will be output.', required=False
        )
        self.parser.add_argument(
            '--access_token_path',
            help='Full path the file where the Zenodo access path is written.\n', required=False
        )
        self.parser.add_argument(
            '--data_file_paths',
            help='Comma seperated paths to the datafiles that should be uploaded as part of the Zenodo submission',
            required=False
        )
        self.parser.add_argument(
            '--meta_title',
            help='The title of the submission',
            required=False
        )
        self.parser.add_argument(
            '--meta_description',
            help='The description for the submission. This can either be a string or a path to a plain text file. '
                 'If a path to a plain text file is given, the contents of that file will be used as the description.',
            required=False
        )
        self.parser.add_argument(
            '--references',
            help='The full path to a file that contains the reference to be included in the metadata. '
                 'Each reference should be placed on a sinlge line.', required=False)

    def _make_creator_array(self):
        """
        Make the creator array that will be passed to the zenodo meta data item.
        This should be a list, that contains one dictionary per person with the keys
        name: Name of creator in the format Family name, Given names
        affiliation: Affiliation of creator (optional).
        orcid: ORCID identifier of creator (optional).
        In future it may be that we use the ORCID to get the affiliation rather than the affiliations that are
        provided with the TARA-PACIFIC_authors document.
        """
        creator_array = []
        for author in self.author_order:
            author_dict = {}
            author_dict['name'] = f'{self.author_info_df.at[author, "last name"]}, {self.author_info_df.at[author, "first name"]}'
            if self.author_info_df.at[author, 'affiliation'] != 'not-provided':
                author_dict['affiliation'] = self.author_info_df.at[author, 'affiliation']
            if self.author_info_df.at[author, 'ORCID'] != 'not-provided':
                author_dict['orcid'] = self.author_info_df.at[author, 'ORCID']
            creator_array.append(author_dict)
        return creator_array

    def do_zenodo_submission(self):
        """
        Create the Zenodo submission using their API documented here:
        https://developers.zenodo.org/#quickstart-upload
        """

        bucket_url, deposition_id = self._create_blank_deposition()

        if self.data_file_path_list:
            filename_list = self._prepare_filenames_and_paths()
            self._upload_files_to_deposition(bucket_url, filename_list)

        # Now add meta data to the deposition
        data = self._make_meta_data_object()
        meta_data_response = self._associate_meta_data(data, deposition_id)

        # Print success, inform user to publish manually
        print('Your submission has been successfully uploaded.\n'
              'Please verify and publish it using the Zenodo.org web interface from inside your account.\n'
              f'Or using this link: {meta_data_response.json()["links"]["html"]} (you will need to be signed in).')

    def _prepare_filenames_and_paths(self):
        # Prepare the filenames and paths that will be used to upload the data
        print('Starting Zenodo submission\n')
        filename_list = []
        for file_path in self.data_file_path_list:
            filename_list.append(ntpath.basename(file_path))
        return filename_list

    def _create_blank_deposition(self):
        # Create a blank deposition
        # TODO check there isn't already a deposition in progress so that the user doesn't keep making new ones
        print('Creating a new deposition')
        create_blank_depo_response = requests.post(
            'https://zenodo.org/api/deposit/depositions',
            params={'access_token': self.access_token}, json={}
        )
        bucket_url = create_blank_depo_response.json()["links"]["bucket"]
        deposition_id = create_blank_depo_response.json()['id']
        print(f'Successful. Deposition ID is {deposition_id}')
        return bucket_url, deposition_id

    def _upload_files_to_deposition(self, bucket_url, filename_list):
        # Add a file to the deposition
        # We pass the file object (fp) directly to the request as the 'data' to be uploaded.
        # The target URL is a combination of the buckets link with the desired filename seperated by a slash.
        print('\nUploading files:')
        for path, filename in zip(self.data_file_path_list, filename_list):
            print(f'\t{path}')
            with open(path, "rb") as fp:
                r = requests.put(f"{bucket_url}/{filename}",
                                 data=fp,
                                 # No headers included in the request, since it's a raw byte request
                                 params={'access_token': self.access_token},
                                 )
        print('Upload complete\n')

    def _associate_meta_data(self, data, deposition_id):
        print('Submitting meta data')
        meta_data_response = requests.put(f'https://zenodo.org/api/deposit/depositions/{deposition_id}',
                         params={'access_token': self.access_token}, data=json.dumps(data),
                         headers={"Content-Type": "application/json"})
        print('Submission of meta data complete\n')
        return meta_data_response

    def _make_meta_data_object(self):
        data = {
            'metadata': {
                'title': self.meta_title,
                'upload_type': 'dataset',
                'description': self.meta_description,
                'access_right': 'restricted',
                'license': 'CC-BY-4.0',
                'access_conditions': 'Any Tara Pacific Expedition participant may request access.',
                'communities': [{'identifier': 'tarapacific'}],
                'version': '1',
                'language': 'eng',
                'creators': self.creator_array,
                'notes': self._get_notes(),
                'references': self.references
            }
        }
        print('Meta information is:')
        print(data)
        print('\n')
        return data

    def output_author_info(self):
        author_string_w_o_affiliation_numbers = self._create_author_without_affiliation_string()

        author_string_w_affiliation_numbers = self._create_author_with_affiliation_string()

        affiliations_new_lines, affiliations_one_line = self._create_affiliation_strings()

        self._write_out_author_and_affiliations(
            affiliations_new_lines, affiliations_one_line,
            author_string_w_affiliation_numbers, author_string_w_o_affiliation_numbers
        )

    def _create_author_without_affiliation_string(self):
        # First output the two variations of the author lists
        author_string_list_w_o_affiliation_numbers = []
        for author in self.author_order:
            author_string_list_w_o_affiliation_numbers.append(
                f'{self.author_info_df.at[author, "last name"]}, {self.author_info_df.at[author, "first name initial(s)"]}')
        author_string_w_o_affiliation_numbers = '; '.join(author_string_list_w_o_affiliation_numbers)
        return author_string_w_o_affiliation_numbers

    def _create_author_with_affiliation_string(self):
        author_string_list_w_affiliation_numbers = []
        for author in self.author_order:
            # first get the affiliation string
            affil_super_script_list = []
            for affil_num in self.author_to_affil_num_list_dict[author]:
                affil_super_script_list.append(self._superscript(affil_num))
            sup_affil_string = '˒'.join(affil_super_script_list)
            author_string_list_w_affiliation_numbers.append(
                f'{self.author_info_df.at[author, "last name"]}, '
                f'{self.author_info_df.at[author, "first name initial(s)"]}{sup_affil_string}'
            )
        author_string_w_affiliation_numbers = '; '.join(author_string_list_w_affiliation_numbers)
        return author_string_w_affiliation_numbers

    def _create_affiliation_strings(self):
        # Then output the affiliations
        affiliations_one_line = '; '.join(
            [
                f'{affil_num + 1}-{self.affil_num_to_affil_str_dict[affil_num + 1]}' for
                affil_num in range(len(self.affiliation_list))
            ]
        )
        affiliations_new_lines = ';\n'.join(
            [
                f'{affil_num + 1}-{self.affil_num_to_affil_str_dict[affil_num + 1]}' for
                affil_num in range(len(self.affiliation_list))
            ]
        )
        return affiliations_new_lines, affiliations_one_line

    def _write_out_author_and_affiliations(self, affiliations_new_lines, affiliations_one_line,
                                           author_string_w_affiliation_numbers, author_string_w_o_affiliation_numbers):
        print('\nAuthor string without affiliation numbers output to:')
        print(f"\t{os.path.join(self.output_dir, f'author_string_w_o_affiliation_numbers.txt')}")
        with open(os.path.join(self.output_dir, f'author_string_w_o_affiliation_numbers.txt'), 'w') as f:
            for line in author_string_w_o_affiliation_numbers:
                f.write(line)
        print('\nAuthor string with affiliation numbers output to:')
        print(f"\t{os.path.join(self.output_dir, f'author_string_w_affiliation_numbers.txt')}")
        with open(os.path.join(self.output_dir, f'author_string_w_affiliation_numbers.txt'), 'w') as f:
            for line in author_string_w_affiliation_numbers:
                f.write(line)
        print('\nAuthor affiliations on one line output to:')
        print(f"\t{os.path.join(self.output_dir, f'affiliations_one_line.txt')}")
        with open(os.path.join(self.output_dir, f'affiliations_one_line.txt'), 'w') as f:
            for line in affiliations_one_line:
                f.write(line)
        print('\nAuthor affiliations on new lines output to:')
        print(f"\t{os.path.join(self.output_dir, f'affiliations_new_lines.txt')}")
        with open(os.path.join(self.output_dir, f'affiliations_new_lines.txt'), 'w') as f:
            for line in affiliations_new_lines:
                f.write(line)

    def _make_affiliations_dicts(self):
        """
        Produce the affiliation list in order and three dicts:
        affil_str_to_affil_num_dict = {}
        affil_num_to_affil_str_dict = {}
        author_to_affil_num_list_dict = defaultdict(list)

        We want to have each affiliation listed only once and numbered in order of the authors
        The affiliation numbers should start at 1
        """
        affiliation_list = []
        affil_str_to_affil_num_dict = {}
        affil_num_to_affil_str_dict = {}
        author_to_affil_num_list_dict = defaultdict(list)

        for author in self.author_order:
            # Get the affiliation of the author
            # Four authors have two affiliations
            try:
                author_affil_list = [self.author_info_df.at[author, 'affiliation']]
            except KeyError as e:
                raise RuntimeWarning(f'An affiliation could not be found for {author}\n'
                                     f'No affiliation will be associated.')

            for affil_string in author_affil_list:
                if affil_string not in affiliation_list:
                    affiliation_list.append(affil_string)
                    affil_str_to_affil_num_dict[affil_string] = len(affiliation_list)
                    author_to_affil_num_list_dict[author].append(len(affiliation_list))
                    affil_num_to_affil_str_dict[len(affiliation_list)] = affil_string
                else:
                    author_to_affil_num_list_dict[author].append(affil_str_to_affil_num_dict[affil_string])
        return affiliation_list, affil_str_to_affil_num_dict, affil_num_to_affil_str_dict, author_to_affil_num_list_dict

    def _make_author_order(self):
        df = pd.read_excel(io=self.excel_path, sheet_name=self.target_sheet_name, header=0)
        # Drop any rows that contain only nan, these may be the 'filtering' rows that excel inserts
        df.dropna(axis='index', how='all', inplace=True)
        # Drop any rows that have a score of 0
        df = df[df['sum'] > 0]
        df = df.fillna(value=0)
        # Keep only the useful cols
        df = df[
            ['last name', 'first name', 'First author(s)', 'Contributing authors list #1',
             'Contributing authors list #2', 'Consortium Coordinators', 'Scientific Directors',
             'Contributing authors list #3', 'sum']
        ]
        # Create the same index as for the name, orchid and affiliation df
        name_index = [last_name + first_name[0] for last_name, first_name in zip(df['last name'], df['first name'])]
        # Check that the last names are unique
        assert (len(name_index) == len(set(name_index)))
        df.index = name_index

        # The author order is then generated by going column by column
        # within each column going in order of top to bottom.
        # An author will only be added once obviously.
        # Where authors appear in two authorship categories, they will be placed into the first category they
        # appear in the order of
        # ['First author(s)', 'Contributing authors list #1',
        # 'Contributing authors list #2', 'Consortium Coordinators']
        # UNLESS the author appears in the Scientific Directors category or the Contributing authors list #3 in which
        # case they will be placed in this category.

        author_order_list = []
        for author_category in self.author_categories:

            if author_category not in  ['Scientific Directors', 'Contributing authors list #3']:
                # Then we are working with one of the first 4 author categories
                for author in df.index:
                    if author not in author_order_list and df.at[author, author_category] > 0 and df.at[author, 'Scientific Directors'] == 0 and df.at[author, 'Contributing authors list #3'] == 0:
                        # Then the author has not yet been placed into the author list
                        # The author is listed in the given author_category
                        # and the author is not listed in the scientific directors or contributing #3 categories
                        author_order_list.append(author)
            else:
                for author in df.index:
                    # Then we are in the scientific director or contributing #3 categories
                    if author not in author_order_list and df.at[author, author_category] > 0:
                        author_order_list.append(author)

        assert(len(author_order_list) == len(df.index))

        return author_order_list

    def _make_author_info_df(self):
        """
        Make a df where index is lastname with first letter of initial appended, and has cols
        ['last name', 'first name', 'first name initial(s)', 'affiliation', 'ORCID']
        """
        df = pd.read_excel(io=self.excel_path, sheet_name='Template', header=0)
        # Drop any rows that contain only nan, these may be the 'filtering' rows that excel inserts
        df.dropna(axis='index', how='all', inplace=True)
        df = df[['last name', 'first name', 'first name initial(s)', 'affiliation', 'ORCID']]
        # last names are not unique so create a key from the last name and fist initial
        name_index = [last_name + first_name[0] for last_name, first_name in zip(df['last name'], df['first name'])]
        # Check that the last names are unique
        assert(len(name_index) == len(set(name_index)))
        df.index = name_index
        df.at['PogoreutzC', 'affiliation'] = 'Department of Biology, University of Konstanz, 78457 Konstanz, Germany'
        df.loc['ClayssenQ'] = ['Clayssen', 'Quentin', 'C.', 'not-provided', 'not-provided']
        return df

    def _superscript(self, number_to_convert):
        """
        Convert the provided number into superscript font for the author and affiliation output text files.
        For getting the superscript and subscript numbers:
        https://stackoverflow.com/questions/8651361/how-do-you-print-superscript-in-python
        """
        sup_map = {
            "0": "⁰", "1": "¹", "2": "²", "3": "³", "4": "⁴", "5": "⁵", "6": "⁶",
            "7": "⁷", "8": "⁸", "9": "⁹"}
        num = str(number_to_convert)
        sup_num = ''
        for c in num:
            sup_num += sup_map[c]
        return sup_num

    def _get_notes(self):
        return "Special thanks to the Tara Ocean Foundation, the R/V Tara crew and the Tara Pacific Expedition " \
               "Participants (https://doi.org/10.5281/zenodo.3777760). We are keen to thank the commitment of " \
               "the following institutions for their financial and scientific support that made this unique Tara " \
               "Pacific Expedition possible: CNRS, PSL, CSM, EPHE, Genoscope, CEA, Inserm, Université Côte d'Azur, " \
               "ANR, agnès b., UNESCO-IOC, the Veolia Foundation, the Prince Albert II de Monaco Foundation, " \
               "Région Bretagne, Billerudkorsnas, AmerisourceBergen Company, Lorient Agglomération, Oceans by " \
               "Disney, L'Oréal, Biotherm, France Collectivités, Fonds Français pour l'Environnement Mondial (FFEM), " \
               "Etienne Bourgois, and the Tara Ocean Foundation teams. Tara Pacific would not exist without the " \
               "continuous support of the participating institutes. The authors also particularly thank Serge " \
               "Planes, Denis Allemand, and the Tara Pacific consortium."

aie = AuthorInfoExtraction()
aie.output_author_info()
if aie.submission:
    # At this point we have all of the objects we need to create the Zenodo submission object.
    aie.do_zenodo_submission()
else:
    print('\nSkipping Zenodo submission.\nTo do the Zenodo submission pass --submission to this script.')
